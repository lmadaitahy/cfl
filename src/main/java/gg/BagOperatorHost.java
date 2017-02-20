package gg;


import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;


// ha nem egyszeru forward a communication pattern, akkor kell egy partitionCustom, hogy az eventeket broadcastolni tudjuk
//  - pl. amikor megvaltozik a parallelism
//  - keyBy helyett is partitionCustom fog kelleni

// kiserletek:
//  - ConnectedComponents
//    - kis adattal, pl. egy hosszu linearis graffal
//    - nagy adattal, hogy a mutable bag-et be tudjuk mutatni
//  - bonyolult control flow, ld. doc


//todo:
// kell egy olyan source, ami DataStream-bol csinal bag-et.
//  - azaz minden partitionje emittal eloszor egy start-ot, aztan amikor a vegere ert a DataStream akkor utana egy end-et
//  - es igy a szokasos Flink DataStream-es source-okkal fogok tudni olvasni


public class BagOperatorHost<IN, OUT>
		extends AbstractStreamOperator<ElementOrEvent<OUT>>
		implements OneInputStreamOperator<ElementOrEvent<IN>,ElementOrEvent<OUT>>, Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(BagOperatorHost.class);

	private BagOperator<IN,OUT> op;
	private int bbId;
	private int inputBbId;
	private int inputParallelism = -1;

	// ---------------------- Initialized in setup (i.e., on TM):

	private byte subpartitionId;

	private CFLManager man;
	private MyCFLCallback cb;

	private InputSubpartition<IN>[] inputSubpartitions;

	private MyCollector outCollector;

	//private static HashMap<BagOperator, Integer> instanceNums = new HashMap<>();

	// ----------------------

	private List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	private Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk
	private int inputCFLSize = -1; // always -1 when not working on an output bag
	private int finishedSubpartitionCounter = -1; // always -1 when not working on an output bag


	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId, int inputBbId) {
		this.op = op;
		this.bbId = bbId;
		this.inputBbId = inputBbId;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId, int inputBbId, int inputParallelism) {
		this(op,bbId,inputBbId);
		this.inputParallelism = inputParallelism;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<OUT>>> output) {
		super.setup(containingTask, config, output);

		//LOG.info("BagOperatorHost.setup");

		this.subpartitionId = (byte)getRuntimeContext().getIndexOfThisSubtask();

		if (inputParallelism == -1) {
			inputParallelism = CFLManager.numAllSlots;
		}
		inputSubpartitions = new InputSubpartition[inputParallelism];
		for(int i=0; i<inputSubpartitions.length; i++){
			inputSubpartitions[i] = new InputSubpartition<>();
		}

		outCFLSizes = new ArrayDeque<>();

		outCollector = new MyCollector();
		op.giveOutputCollector(outCollector);

		man = CFLManager.getSing();
		cb = new MyCFLCallback();
		man.subscribe(cb);
	}

	@Override
	public void open() throws Exception {
		super.open();
	}

	@Override
	synchronized public void processElement(StreamRecord<ElementOrEvent<IN>> streamRecord) throws Exception {

		ElementOrEvent<IN> eleOrEvent = streamRecord.getValue();
		int spId = eleOrEvent.subPartitionId;
		InputSubpartition<IN> sp = inputSubpartitions[spId];

		if (eleOrEvent.element != null) {

			IN ele = eleOrEvent.element;
			if(!sp.damming) {
				op.pushInElement(ele);
			} else {
				sp.buffers.get(sp.buffers.size()-1).add(ele);
			}

		} else {

			assert eleOrEvent.event != null;
			ElementOrEvent.Event ev = eleOrEvent.event;
			switch (eleOrEvent.event.type) {
				case START:
					assert sp.status == InputSubpartition.Status.CLOSED;
					sp.status = InputSubpartition.Status.OPEN;
					if(inputCFLSize != -1) {
						if(inputCFLSize == ev.cflSize){ // It is just what we need for the current out bag
							sp.damming = false;
						} else { // It doesn't match our current out bag
							sp.damming = true;
							sp.cflSizes.add(ev.cflSize);
							sp.buffers.add(new ArrayList<>());
						}
					} else { // We are not working on any out bag at the moment
						sp.damming = true;
						sp.cflSizes.add(ev.cflSize);
						sp.buffers.add(new ArrayList<>());
					}
					break;
				case END:
					assert sp.status == InputSubpartition.Status.OPEN;
					sp.status = InputSubpartition.Status.CLOSED;
					if(!sp.damming && !sp.buffers.isEmpty() && sp.cflSizes.get(sp.cflSizes.size()-1) == inputCFLSize){
						// ez ugyebar az az eset amikor kozben valtott a damming false-ra
						giveBufferToBagOperator(sp, sp.cflSizes.size()-1);
					} else {
						assert !sp.damming || !sp.buffers.isEmpty();
						if(!sp.damming) {
							// ez az az eset, amikor egyaltalan nem volt dammelve
							incAndCheckFinishedSubpartitionCounter();
						}
					}
					break;
				default:
					assert false;
					break;
			}

		}
	}

	private class MyCollector implements BagOperatorOutputCollector<OUT> {
		@Override
		public void collectElement(OUT e) {
			output.collect(
					new StreamRecord<ElementOrEvent<OUT>>(
							new ElementOrEvent<OUT>(subpartitionId, e), 0));
		}

		@Override
		public void closeBag() {
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, outCFLSizes.peek());
			output.collect(
					new StreamRecord<ElementOrEvent<OUT>>(
							new ElementOrEvent<OUT>(subpartitionId, event), 0));

			outCFLSizes.poll();
			if(outCFLSizes.size()>0) {
				startOutBag();
			}
		}
	}

	// i: buffer index in inputSubpartitions
	synchronized private void giveBufferToBagOperator(InputSubpartition<IN> sp, int i) {
		// Note: We get here only after all the elements for this bag from this input subpartition have arrived

		for(IN e: sp.buffers.get(i)) {
			op.pushInElement(e);
		}

		// todo: remove buffer if bonyolult CFG-s condition

		incAndCheckFinishedSubpartitionCounter();
	}

	synchronized private void incAndCheckFinishedSubpartitionCounter() {
		finishedSubpartitionCounter++;
		if(finishedSubpartitionCounter == inputParallelism) {
			op.closeInBag();
			inputCFLSize = -1;
			finishedSubpartitionCounter = -1;
		}
	}

	synchronized private void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer cflSize = outCFLSizes.peek();

		// figure out the input bag ID
		{
			int i;
			for (i = cflSize - 1; latestCFL.get(i) != inputBbId; i--) {}
			assert inputCFLSize == -1;
			inputCFLSize = i + 1;
		}

		assert finishedSubpartitionCounter == -1;
		finishedSubpartitionCounter = 0;

		op.OpenInBag();

		// Tell the input subpartitions what to do:
		//  - Find a buffer that has the appropriate id
		//    - If it is a finished buffer, then give all the elements to the BagOperator
		//    - If it is the last buffer and not finished, then remove the dam
		//  - If there is no appropriate buffer, then we do nothing for now
		for(InputSubpartition<IN> sp: inputSubpartitions) {
			assert sp.cflSizes.size() == sp.buffers.size();
			int i;
			for(i=0; i<sp.cflSizes.size(); i++) {
				if(sp.cflSizes.get(i) == inputCFLSize)
					break;
			}
			if(i<sp.cflSizes.size()) { // we have found an appropriate buffer
				if(i<sp.cflSizes.size()-1 || sp.status == InputSubpartition.Status.CLOSED) { // it's finished
					giveBufferToBagOperator(sp, i);
				} else { // it's the last one and not finished
					sp.damming = false;
				}
			}
		}
	}

	private class MyCFLCallback implements CFLCallback {
		public void notify(List<Integer> cfl) {
			synchronized (BagOperatorHost.this) {
				latestCFL = cfl;
				if (cfl.get(cfl.size() - 1) == bbId) {
					outCFLSizes.add(cfl.size());
					if (outCFLSizes.size() == 1) { // jelenleg nem dolgozunk epp, mert ures volt
						startOutBag();
					}
				}

				//todo: conditional outputs
			}
		}
	}
}
