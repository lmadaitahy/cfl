package gg;


import org.apache.flink.streaming.api.datastream.InputParaSettable;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;



public class PhiNode<T>
		extends AbstractStreamOperator<ElementOrEvent<T>>
		implements OneInputStreamOperator<ElementOrEvent<T>,ElementOrEvent<T>>,
			InputParaSettable<ElementOrEvent<T>,ElementOrEvent<T>>,
			Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(PhiNode.class);

	private int bbId;
	private int inputParallelism = -1;
	private int terminalBBId = -2;

	// ---------------------- Initialized in setup (i.e., on TM):

	private short subpartitionId;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	// ----------------------

	private List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	private Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk
	private int inputCFLSize = -1; // always -1 when not working on an output bag
	private int finishedSubpartitionCounter = -1; // always -1 when not working on an output bag

	private ArrayList<Input> inputs;

	private boolean terminalBBReached;

	public PhiNode(int bbId) {
		this.bbId = bbId;
		this.inputs = new ArrayList<>();
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	public PhiNode<T> addInput(int id, int bbId) {
		assert id == inputs.size();
		inputs.add(new Input(id, bbId));
		return this;
	}

	@Override
	public void setInputPara(int p) {
		this.inputParallelism = p;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<T>>> output) {
		super.setup(containingTask, config, output);

		//LOG.info("BagOperatorHost.setup");

		this.subpartitionId = (short)getRuntimeContext().getIndexOfThisSubtask();

		if (inputParallelism == -1) {
			//inputParallelism = CFLManager.numAllSlots;
			throw new RuntimeException("inputParallelism is not set. Use bt instead of transform!");
		}

		for(Input inp: inputs) {
			inp.inputSubpartitions = new InputSubpartition[inputParallelism];
			for (int i = 0; i < inp.inputSubpartitions.length; i++) {
				inp.inputSubpartitions[i] = new InputSubpartition<>();
			}
		}

		outCFLSizes = new ArrayDeque<>();

		terminalBBReached = false;

		cflMan = CFLManager.getSing();

		cflMan.specifyTerminalBB(terminalBBId);
	}

	@Override
	public void open() throws Exception {
		super.open();

		cb = new MyCFLCallback();
		cflMan.subscribe(cb);
	}

	@Override
	synchronized public void processElement(StreamRecord<ElementOrEvent<T>> streamRecord) throws Exception {

		ElementOrEvent<T> eleOrEvent = streamRecord.getValue();
		assert eleOrEvent.logicalInputId != -1; // (kell egy extra map, ami kitolti)
		Input input = inputs.get(eleOrEvent.logicalInputId);
		InputSubpartition<T> sp = input.inputSubpartitions[eleOrEvent.subPartitionId];

		if (eleOrEvent.element != null) {

			T ele = eleOrEvent.element;
			if(!sp.damming) {
				sendElement(ele);
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
						sendBuffer(sp, sp.cflSizes.size()-1);
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

	private void sendElement(T ele) {
		output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, ele, (byte)-1), 0));
	}

	// i: buffer index in inputSubpartitions
	synchronized private void sendBuffer(InputSubpartition<T> sp, int i) {
		// Note: We get here only after all the elements for this bag from this input subpartition have arrived

		sp.buffers.get(i).forEach(this::sendElement);

		// todo: remove buffer if bonyolult CFG-s condition

		incAndCheckFinishedSubpartitionCounter();
	}

	synchronized private void incAndCheckFinishedSubpartitionCounter() {
		finishedSubpartitionCounter++;
		if(finishedSubpartitionCounter == inputParallelism) {
			inputCFLSize = -1;
			finishedSubpartitionCounter = -1;

			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, outCFLSizes.peek());
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, (byte)-1), 0));

			outCFLSizes.poll();
			if(outCFLSizes.size() > 0) { // ha van jelenleg varakozo munka
				startOutBag();
			} else if (terminalBBReached) { // ha nincs jelenleg varakozo munka es mar nem is jon tobb
				cflMan.unsubscribe(cb);
			}
		}
	}

	synchronized private void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer cflSize = outCFLSizes.peek();

		int logicalInput = -1; // which logical input are we processing

		// figure out the input bag ID and which logical input
		// Scan the CFL backwards, and find the first occurrence of a bbId of any of our logical inputs
		assert inputCFLSize == -1;
		{
			int i;
			for (i = cflSize - 2; ; i--) {
				assert i >= 0;
				int j = 0;
				boolean brk = false;
				for (Input inp: inputs) {
					if(inp.bbId == latestCFL.get(i)) {
						assert logicalInput == -1; // because two logical inputs can't have the same basic block id
						logicalInput = j;
						brk = true;
					}
					j++;
				}
				if(brk){
					break;
				}
			}
			inputCFLSize = i + 1;
		}

		LOG.info("Phi-node starts processing from logical input " + logicalInput + ", with CFLSize " + inputCFLSize);


		assert finishedSubpartitionCounter == -1;
		finishedSubpartitionCounter = 0;

		assert latestCFL.get(cflSize - 1) == bbId;

		ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, cflSize);
		output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, (byte)-1), 0));

		// Tell the input subpartitions what to do:
		//  - In the current logical input:
		//    - Find a buffer that has the appropriate id
		//      - If it is a finished buffer, then give all the elements to the BagOperator
		//      - If it is the last buffer and not finished, then remove the dam
		//    - If there is no appropriate buffer, then we do nothing for now
		//  - In the other logical input we set damming to true on every input subpartition
		for(int j = 0; j < inputs.size(); j++) {
			if (j == logicalInput) {
				for (InputSubpartition<T> sp : inputs.get(j).inputSubpartitions) {
					assert sp.cflSizes.size() == sp.buffers.size();
					int i;
					for (i = 0; i < sp.cflSizes.size(); i++) {
						if (sp.cflSizes.get(i) == inputCFLSize)
							break;
					}
					if (i < sp.cflSizes.size()) { // we have found an appropriate buffer
						if (i < sp.cflSizes.size() - 1 || sp.status == InputSubpartition.Status.CLOSED) { // it's finished
							sendBuffer(sp, i);
						} else { // it's the last one and not finished
							sp.damming = false;
						}
					}
				}
			} else {
				for (InputSubpartition<T> sp : inputs.get(j).inputSubpartitions) {
					sp.damming = true;
				}
			}
		}
	}

	private class MyCFLCallback implements CFLCallback {

		public void notify(List<Integer> cfl) {
			synchronized (PhiNode.this) {
				latestCFL = cfl;

				LOG.info("CFL notification: " + latestCFL);

				if (cfl.get(cfl.size() - 1) == bbId) {
					outCFLSizes.add(cfl.size());
					if (outCFLSizes.size() == 1) { // jelenleg nem dolgozunk epp (ezt onnan tudjuk, hogy ures volt az outCFLSizes)
						startOutBag();
					}
				}
			}
		}

		@Override
		public void notifyTerminalBB() {
			terminalBBReached = true;
			if (outCFLSizes.isEmpty()) {
				cflMan.unsubscribe(cb);
			}
		}
	}

	// there will be one of these for each input-alternative of the phi-node
	private class Input implements Serializable {

		int id; // marmint sorszam az inputs tombben
		int bbId;
		InputSubpartition<T>[] inputSubpartitions;


		public Input(int id, int bbId) {
			this.id = id;
			this.bbId = bbId;
		}
	}
}
