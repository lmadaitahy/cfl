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
import java.util.List;
import java.util.Queue;


public class BagOperatorHost<IN, OUT>
		extends AbstractStreamOperator<ElementOrEvent<OUT>>
		implements OneInputStreamOperator<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			InputParaSettable<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperatorHost.class);

	private BagOperator<IN,OUT> op;
	private int bbId;
	private int inputParallelism = -1;
	private String name;
	private int terminalBBId = -2;

	// ---------------------- Initialized in setup (i.e., on TM):

	private short subpartitionId;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	protected ArrayList<Input> inputs;

	// ----------------------

	protected List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	private Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk

	private ArrayList<Out> outs = new ArrayList<>(); // conditional and normal outputs

    private volatile boolean terminalBBReached;

	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId) {
		this.op = op;
		this.bbId = bbId;
		this.inputs = new ArrayList<>();
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		assert this.terminalBBId >= 0;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	public BagOperatorHost<IN,OUT> addInput(int id, int bbId, boolean inputInSameBlock) {
		assert id == inputs.size();
		inputs.add(new Input(id, bbId, inputInSameBlock));
		return this;
	}

	@Override
	public void setInputPara(int p) {
		this.inputParallelism = p;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		this.op.setName(name);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<OUT>>> output) {
		super.setup(containingTask, config, output);

		this.subpartitionId = (short)getRuntimeContext().getIndexOfThisSubtask();

		if (inputParallelism == -1) {
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

		op.giveOutputCollector(new MyCollector());

		cflMan = CFLManager.getSing();

		cflMan.specifyTerminalBB(terminalBBId);
	}

	@Override
	public void open() throws Exception {
		super.open();

		Thread.sleep(100); // this is a workaround for the buffer pool destroyed error

		cb = new MyCFLCallback();
		cflMan.subscribe(cb);
	}

	@Override
	synchronized public void processElement(StreamRecord<ElementOrEvent<IN>> streamRecord) throws Exception {

		ElementOrEvent<IN> eleOrEvent = streamRecord.getValue();
		if (inputs.size() == 1) {
			assert eleOrEvent.logicalInputId == -1 || eleOrEvent.logicalInputId == 0;
			eleOrEvent.logicalInputId = 0; // This is to avoid having to have an extra map to set this for even one-input operators
		}
		assert eleOrEvent.logicalInputId != -1; // (kell egy extra map, ami kitolti (ha nem egyinputos))
		Input input = inputs.get(eleOrEvent.logicalInputId);
		InputSubpartition<IN> sp = input.inputSubpartitions[eleOrEvent.subPartitionId];

		if (eleOrEvent.element != null) {
			IN ele = eleOrEvent.element;
			sp.buffers.get(sp.buffers.size()-1).add(ele);
			if(!sp.damming) {
				op.pushInElement(ele, eleOrEvent.logicalInputId);
			}
		} else {

			assert eleOrEvent.event != null;
			ElementOrEvent.Event ev = eleOrEvent.event;
			switch (eleOrEvent.event.type) {
				case START:
					assert sp.status == InputSubpartition.Status.CLOSED;
					sp.status = InputSubpartition.Status.OPEN;
					assert sp.cflSizes.size() == sp.buffers.size(); // egyutt mozognak
					sp.cflSizes.add(ev.cflSize);
					sp.buffers.add(new ArrayList<>());
					if(input.inputCFLSize != -1) {
						if(input.inputCFLSize == ev.cflSize){ // It is just what we need for the current out bag
							sp.damming = false;
						} else { // It doesn't match our current out bag
							sp.damming = true;
						}
					} else { // We are not working on any out bag at the moment
						sp.damming = true;
					}
					break;
				case END:
					assert sp.status == InputSubpartition.Status.OPEN;
					sp.status = InputSubpartition.Status.CLOSED;
					if(!sp.damming) {
						incAndCheckFinishedSubpartitionCounter(eleOrEvent.logicalInputId);
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
			assert outs.size() != 0; // If the operator wants to emit an element but there are no outs, then we just probably forgot to call .out()
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

			outCFLSizes.remove();
			if(outCFLSizes.size() > 0) { // ha van jelenleg varakozo munka
				// Note: ettol el fog dobodni az Outok buffere, de ez nem baj, mert aminek el kellett mennie az mar elment
				startOutBag();
			} else {
                if (terminalBBReached) { // ha nincs jelenleg varakozo munka es mar nem is jon tobb
                    cflMan.unsubscribe(cb);
                }
            }
		}

		@Override
		public void appendToCfl(int bbId) {
			cflMan.appendToCFL(bbId);
		}
	}

	// i: buffer index in inputSubpartitions
	synchronized private void giveBufferToBagOperator(InputSubpartition<IN> sp, int i, int logicalInputId) {
		for(IN e: sp.buffers.get(i)) {
			op.pushInElement(e, logicalInputId);
		}
	}

	synchronized private void incAndCheckFinishedSubpartitionCounter(int inputId) {
		// todo: remove buffer if bonyolult CFG-s condition
		Input input = inputs.get(inputId);
		assert input.finishedSubpartitionCounter >= 0;
		input.finishedSubpartitionCounter++;
		if (input.finishedSubpartitionCounter == inputParallelism) {
			input.inputCFLSize = -1;
			input.finishedSubpartitionCounter = -1;
			op.closeInBag(inputId);
		}
	}

	synchronized private void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer outCFLSize = outCFLSizes.peek();

		assert latestCFL.get(outCFLSize - 1) == bbId;

		// Treat outputs
		for(Out o: outs) {
			boolean targetReached = false;
			for (int i = outCFLSize; i < latestCFL.size(); i++) {
				if(latestCFL.get(i) == o.targetBbId) {
					targetReached = true;
				}
			}
			o.cflSize = outCFLSize;
			if(!targetReached && !o.normal){
				o.state = OutState.DAMMING;
				o.buffer = new ArrayList<>();
			} else {
				o.sendStart(outCFLSize);
				o.state = OutState.FORWARDING;
			}
		}

		// Tell the BagOperator that we are opening a new bag
		op.openOutBag();

		for (Input input: inputs) {
			assert input.finishedSubpartitionCounter == -1;

			for (InputSubpartition<IN> sp : input.inputSubpartitions) {
				sp.damming = true; // ezek kozul ugyebar nemelyiket majd false-ra allitja az activateLogicalInput mindjart
			}
		}

		chooseLogicalInputs(outCFLSize);
	}

	// Note: this activates all the logical inputs. (Cf. the override in PhiNode, which activates only one.)
	protected void chooseLogicalInputs(int outCFLSize) {
		// figure out the input bag ID
		for (Input input: inputs) {
			assert input.inputCFLSize == -1;
			if (input.inputInSameBlock) {
				input.inputCFLSize = outCFLSize;
			} else {
				int i;
				for (i = outCFLSize - 2; input.bbId != latestCFL.get(i); i--) {}
				input.inputCFLSize = i + 1;
			}

			activateLogicalInput(input.id);
		}
	}

	// Note: inputCFLSize should be set before this
	protected void activateLogicalInput(int id) {
		// Tell the input subpartitions what to do:
		//  - Find a buffer that has the appropriate id
		//    - Give all the elements to the BagOperator
		//    - If it is finished, then call incAndCheckFinishedSubpartitionCounter
		//    - If it is not finished, then remove the dam
		//  - If there is no appropriate buffer, then we do nothing for now
		op.openInBag(id);
		Input input = inputs.get(id);
		input.finishedSubpartitionCounter = 0;
		for(InputSubpartition<IN> sp: input.inputSubpartitions) {
			assert sp.cflSizes.size() == sp.buffers.size();
			int i;
			for(i = 0; i < sp.cflSizes.size(); i++) {
				if(sp.cflSizes.get(i) == input.inputCFLSize)
					break;
			}
			if(i < sp.cflSizes.size()) { // we have found an appropriate buffer
				giveBufferToBagOperator(sp, i, id);
				if(i < sp.cflSizes.size() - 1) { // not the last one
					assert sp.status == InputSubpartition.Status.CLOSED; // should be finished
					incAndCheckFinishedSubpartitionCounter(id);
				} else { // it's the last one
					if (sp.status == InputSubpartition.Status.CLOSED) { // and finished
						incAndCheckFinishedSubpartitionCounter(id);
					} else { // not finished
						// Ez az az eset, amikor kozben vesszuk ki a dam-et.
						// Ujabban ilyenkor rogton odaadjuk a buffer-t, azaz az eddig erkezett elemeket.
						sp.damming = false;
					}
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
								o.sendStart(o.cflSize);
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

        @Override
        public void notifyTerminalBB() {
			LOG.info("CFL notifyTerminalBB");
            terminalBBReached = true;
			if (outCFLSizes.isEmpty()) {
				cflMan.unsubscribe(cb);
			}
        }
    }

    // `normal` means not conditional.
	// If `normal` is false, then we set to damming until we reach its BB.
	// (This means that for example if targetBbId is the same as the operator's BbId, then we wait for the next iteration step.)
	public BagOperatorHost<IN, OUT> out(int splitId, int targetBbId, boolean normal) {
		assert splitId == outs.size();
		outs.add(new Out((byte)splitId, targetBbId, normal));
		return this;
	}

	private enum OutState {IDLE, DAMMING, WAITING, FORWARDING}

	private final class Out implements Serializable {

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

		Out(byte splitId, int targetBbId, boolean normal) {
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

	// Logical input
	protected final class Input implements Serializable {

		int id; // marmint sorszam az inputs tombben
		int bbId;
		boolean inputInSameBlock; // marmint ugy ertve, hogy a kodban elotte (szoval ha utana van ugyanabban a blockban, akkor ez false)
		InputSubpartition<IN>[] inputSubpartitions;
		int finishedSubpartitionCounter = -1; // always -1 when not working on an output bag or when we are not participating in the computation of the current out bag
		int inputCFLSize = -1; // always -1 when not working on an output bag

		public Input(int id, int bbId, boolean inputInSameBlock) {
			this.id = id;
			this.bbId = bbId;
			this.inputInSameBlock = inputInSameBlock;
		}
	}
}
