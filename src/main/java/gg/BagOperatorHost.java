package gg;


import gg.operators.BagOperator;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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


//TODO: Muszaj normalisan megcsinalni a phi-node-okat:
// - azt is tudniuk kell, hogy ha egyszerre jonnek elemek a ket unionozott inputbol, akkor egymas utanra rakja oket
// - meg akkor mar majd vissza lehet csinalni ezt, hogy most egy tomb az inputCFL


public class BagOperatorHost<IN, OUT>
		extends AbstractStreamOperator<ElementOrEvent<OUT>>
		implements OneInputStreamOperator<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			InputParaSettable<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(BagOperatorHost.class);

	private BagOperator<IN,OUT> op;
	private int bbId;
	private HashSet<Integer> inputBbIds;
	private int inputParallelism = -1;
	private boolean inputInSameBlock;

	// ---------------------- Initialized in setup (i.e., on TM):

	private byte subpartitionId;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	private InputSubpartition<IN>[] inputSubpartitions;

	private MyCollector outCollector;

	// ----------------------

	private List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	private Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk
	private int inputCFLSize = -1; // always -1 when not working on an output bag
	private int finishedSubpartitionCounter = -1; // always -1 when not working on an output bag

	private ArrayList<Out> outs = new ArrayList<>(); // conditional and normal outputs

	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId, Integer[] inputBbIds, boolean inputInSameBlock) {
		this.op = op;
		this.bbId = bbId;
		this.inputBbIds = new HashSet<>(Arrays.asList(inputBbIds));
		this.inputInSameBlock = inputInSameBlock;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	@Override
	public void setInputPara(int p) {
		this.inputParallelism = p;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<OUT>>> output) {
		super.setup(containingTask, config, output);

		//LOG.info("BagOperatorHost.setup");

		this.subpartitionId = (byte)getRuntimeContext().getIndexOfThisSubtask();

		if (inputParallelism == -1) {
			//inputParallelism = CFLManager.numAllSlots;
			throw new RuntimeException("inputParallelism is not set. Use bt instead of transform!");
		}
		inputSubpartitions = new InputSubpartition[inputParallelism];
		for(int i=0; i<inputSubpartitions.length; i++){
			inputSubpartitions[i] = new InputSubpartition<>();
		}

		outCFLSizes = new ArrayDeque<>();

		outCollector = new MyCollector();
		op.giveOutputCollector(outCollector);

		cflMan = CFLManager.getSing();
	}

	@Override
	public void open() throws Exception {
		super.open();

		cb = new MyCFLCallback();
		cflMan.subscribe(cb);
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
			for(Out o: outs) {
				assert o.state == OutState.FORWARDING || o.state == OutState.DAMMING;
				if(o.state == OutState.FORWARDING) {
					o.sendElement(e);
				} else {
					o.buffer.add(e);
				}
			}
		}

		@Override
		public void closeBag() {
			for(Out o: outs) {
				switch (o.state) {
					case IDLE:
						assert false;
						break;
					case DAMMING:
						o.state = OutState.WAITING;
						break;
					case WAITING:
						assert false;
						break;
					case FORWARDING:
						if (o.buffer != null) {
							for(OUT e: o.buffer) {
								o.sendElement(e);
							}
						}
						assert o.cflSize == outCFLSizes.peek();
						o.sendEnd(o.cflSize);
						break;
				}
			}

			outCFLSizes.poll();
			if(outCFLSizes.size()>0) {
				// Note: ettol el fog dobodni az Outok buffere, de ez nem baj, mert aminek el kellett mennie az mar elment
				startOutBag();
			}
		}

		@Override
		public void appendToCfl(int bbId) {
			cflMan.appendToCFL(bbId);
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
			inputCFLSize = -1;
			finishedSubpartitionCounter = -1;
			op.closeInBag();
		}
	}

	synchronized private void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer cflSize = outCFLSizes.peek();

		// figure out the input bag ID
		assert inputCFLSize == -1;
		// We include the current BB in the search. This is OK, because a back-edge can only target a phi-node,
		// which won't have to do this.
		if(inputInSameBlock) {
			inputCFLSize = cflSize;
		} else {
			int i;
			for (i = cflSize - 2; !inputBbIds.contains(latestCFL.get(i)); i--) {
			}
			inputCFLSize = i + 1;
		}


		assert finishedSubpartitionCounter == -1;
		finishedSubpartitionCounter = 0;

		assert latestCFL.get(cflSize - 1) == bbId;

		// Treat outputs
		for(Out o: outs) {
			boolean targetReached = false;
			for (int i=cflSize; i<latestCFL.size(); i++) {
				if(latestCFL.get(i) == o.targetBbId) {
					targetReached = true;
				}
			}
			o.cflSize = cflSize;
			if(!targetReached && !o.normal){
				o.state = OutState.DAMMING;
				o.buffer = new ArrayList<>();
			} else {
				o.sendStart(cflSize);
				o.state = OutState.FORWARDING;
			}
		}

		// Tell the BagOperator that we are opening a new bag
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

				LOG.info("CFL notification: " + latestCFL);

				// Note: figyelni kell, hogy itt hamarabb legyen az out-ok kezelese, mint a startOutBag hivas, mert az el fogja dobni a buffereket,
				// es van olyan, hogy ugyanannak a BB-nek az elerese mindket dolgot kivaltja

				for(Out o: outs) {
					if (cfl.get(cfl.size() - 1) == o.targetBbId) {
						switch (o.state) {
							case IDLE:
								break;
							case DAMMING:
								assert outCFLSizes.size() > 0;
								assert o.cflSize == outCFLSizes.peek();
								o.sendStart(o.cflSize);
								o.state = OutState.FORWARDING;
								break;
							case WAITING:
								if (o.buffer != null) {
									for (OUT e : o.buffer) {
										o.sendElement(e);
									}
								}
								o.sendEnd(o.cflSize);
								o.state = OutState.IDLE;
								break;
							case FORWARDING:
								break;
						}
					}
				}

				if (cfl.get(cfl.size() - 1) == bbId) {
					outCFLSizes.add(cfl.size());
					if (outCFLSizes.size() == 1) { // jelenleg nem dolgozunk epp (ezt onnan tudjuk, hogy ures volt az outCFLSizes)
						startOutBag();
					}
				}
			}
		}
	}

	public BagOperatorHost<IN, OUT> out(int splitId, int targetBbId, boolean normal) {
		outs.add(new Out((byte)splitId, targetBbId, normal));
		return this;
	}

	enum OutState {IDLE, DAMMING, WAITING, FORWARDING}

	public final class Out implements Serializable {

		// Egyelore nem csinalunk kulon BB elerese altal kivaltott discardot. (Amugy ha uj out bag van, akkor eldobodik a reginek a buffere igy is.)

		// 4 esetben tortenik valami:
		//  - startOutBag:
		//    - ha nem erte el a targetet, akkor DAMMING, es new Buffer
		//    - ha elerte a targetet vagy normal, akkor sendStart es FORWARDING
		//  - elem jon a BagOperatorbol
		//    - assert DAMMING or FORWARDING, es aztan tesszuk a megfelelot
		//  - vege van a BagOperatorbol jovo bagnek
		//    - IDLE nem lehet
		//    - ha DAMMING, akkor WAITING-re valtunk
		//    - WAITING nem lehet
		//    - ha FORWARDING, akkor megnezzuk, hogy van-e buffer, es ha igen, akkor kuldjuk, es IDLE-re valtunk (ugye ekkor kozben volt D->F valtas)
		//  - a CFL eleri a targetet
		//    - IDLE akkor semmi
		//    - ha DAMMING, akkor sendStart es FORWARDING-ra valtunk
		//    - ha WAITING, akkor elkuldjuk a buffert, es IDLE-re valtunk
		//    - FORWARDING akkor semmi

		private byte splitId = -1;
		int targetBbId = -1;
		boolean normal = false; // jelzi ha nem conditional

		ArrayList<OUT> buffer = null;
		OutState state = OutState.IDLE;
		int cflSize = -1; // The CFL that is being emitted. We need this, because cflSizes becomes empty when we become waiting.

		public Out(byte splitId, int targetBbId, boolean normal) {
			this.splitId = splitId;
			this.targetBbId = targetBbId;
			this.normal = normal;
		}

		void sendElement(OUT e) {
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, e, splitId), 0));
		}

		void sendStart(int cflSize) {
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, cflSize);
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId), 0));
		}

		void sendEnd(int cflSize) {
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, cflSize);
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId), 0));
		}
	}
}
