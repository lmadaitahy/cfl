package gg;


import gg.operators.BagOperator;
import gg.partitioners.Partitioner;
import org.apache.flink.streaming.api.datastream.InputParaSettable;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.gg.NoAutoClose;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


public class BagOperatorHost<IN, OUT>
		extends AbstractStreamOperator<ElementOrEvent<OUT>>
		implements OneInputStreamOperator<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			InputParaSettable<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			NoAutoClose,
			Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperatorHost.class);

	private BagOperator<IN,OUT> op;
	protected int bbId;
	private int inputParallelism = -1;
	private String name;
	private int terminalBBId = -2;
	private CFLConfig cflConfig;
	private int opID = -1;

	// ---------------------- Initialized in setup (i.e., on TM):

	private short subpartitionId = -25;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	protected ArrayList<Input> inputs;

	// ----------------------

	protected List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	protected Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk

	private ArrayList<Out> outs = new ArrayList<>(); // conditional and normal outputs

    private volatile boolean terminalBBReached;

	private HashSet<BagID> notifyCloseInputs = new HashSet<>();
	private HashSet<BagID> notifyCloseInputEmpties = new HashSet<>();

	private boolean consumed = false;

	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId, int opID) {
		this.op = op;
		this.bbId = bbId;
		this.inputs = new ArrayList<>();
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		this.cflConfig = CFLConfig.getInstance();
		assert this.terminalBBId >= 0;
		this.opID = opID;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	public BagOperatorHost<IN,OUT> addInput(int id, int bbId, boolean inputInSameBlock, int opID) {
		assert id == inputs.size();
		inputs.add(new Input(id, bbId, inputInSameBlock, opID));
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
		cflMan.specifyNumToSubscribe(cflConfig.numToSubscribe);
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

		if (CFLConfig.vlog) LOG.info("Operator {" + name + "}[" + subpartitionId +"] processElement " + streamRecord.getValue());

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
			sp.buffers.get(sp.buffers.size()-1).elements.add(ele);
			if(!sp.damming) {
				consumed = true;
				op.pushInElement(ele, eleOrEvent.logicalInputId);
			}
		} else {

			assert eleOrEvent.event != null;
			ElementOrEvent.Event ev = eleOrEvent.event;
			switch (eleOrEvent.event.type) {
				case START:
					assert eleOrEvent.event.assumedTargetPara == getRuntimeContext().getNumberOfParallelSubtasks();
					assert sp.status == InputSubpartition.Status.CLOSED;
					sp.status = InputSubpartition.Status.OPEN;
					sp.addNewBuffer(ev.bagID);
					assert input.opID == ev.bagID.opID;
					//assert input.currentBagID == null || input.currentBagID.equals(ev.bagID); // ezt azert kellett kicommentezni, mert a null-ra allitast kivettem, mert rossz helyen volt
					//assert input.currentBagID.equals(ev.bagID); // Ez meg azert nem igaz, mert van, hogy az activateLogicalInput meg nem fut le, amikor a start mar megerkezik
					// A kov ertekadas azert nem jo, mert igy a notifyCloseInput azt hinne, hogy mar aktivalva lett.
					// De lehet, hogy csak kesobb lesz aktivalva (es akkor majd persze megnezzuk, hogy kaptunk-e mar notifyCloseInput-ot ra).
					//input.currentBagID = ev.bagID;
					// Note: Sometimes the buffer is not really needed: we could check some tricky condition on the
					// control flow graph, but this is not so important for the experiments in the paper.
					if(input.inputCFLSize != -1) {
						if(input.inputCFLSize == ev.bagID.cflSize){ // It is just what we need for the current out bag
							sp.damming = false;
						} else { // It doesn't match our current out bag
							sp.damming = true;
						}
					} else { // We are not working on any out bag at the moment
						sp.damming = true;
					}
					break;
				case END:
					assert eleOrEvent.event.assumedTargetPara == getRuntimeContext().getNumberOfParallelSubtasks();
					assert sp.status == InputSubpartition.Status.OPEN;
					sp.status = InputSubpartition.Status.CLOSED;
					InputSubpartition.Buffer lastBuffer = sp.buffers.get(sp.buffers.size()-1);
					cflMan.consumedLocal(lastBuffer.bagID,lastBuffer.elements.size(),subpartitionId,opID);
					break;
				default:
					assert false;
					break;
			}

		}
	}

	protected void outCFLSizesRemove() {
		outCFLSizes.remove();
	}

	private class MyCollector implements BagOperatorOutputCollector<OUT> {

		private int numElements = 0;

		@Override
		public void collectElement(OUT e) {
			numElements++;
			assert outs.size() != 0; // If the operator wants to emit an element but there are no outs, then we just probably forgot to call .out()
			for(Out o: outs) {
				o.collectElement(e);
			}
		}

		@Override
		public void closeBag() {
			for(Out o: outs) {
				o.closeBag();
			}

			ArrayList<BagID> inputBagIDs = new ArrayList<>();
			for (Input inp: inputs) {
				// a kov. assert akkor mondjuk elromolhat, ha ilyen short-circuit-es jellegu az operator, hogy van, hogy mar akkor
				// lezarja az output bag-et, amikor meg nem kezdodott el minden inputon az input bag, de kesobb meg fog onnan jonni.
				//assert inp.currentBagID != null; // PhiNode-nal nem fasza
				if (inp.activeFor.contains(outCFLSizes.peek())) {
					assert inp.currentBagID != null;
					inputBagIDs.add(inp.currentBagID);
				}
			}
			BagID[] inputBagIDsArr = new BagID[inputBagIDs.size()];
			int i = 0;
			for (BagID b: inputBagIDs) {
				inputBagIDsArr[i++] = b;
			}

			BagID outBagID = new BagID(outCFLSizes.peek(), opID);
			if (numElements > 0 || consumed) {
				cflMan.producedLocal(outBagID, inputBagIDsArr, numElements, getRuntimeContext().getNumberOfParallelSubtasks(), subpartitionId, opID);
			}

			numElements = 0;

			outCFLSizesRemove();
			if(outCFLSizes.size() > 0) { // ha van jelenleg varakozo munka
				if (CFLConfig.vlog) LOG.info("Out.closeBag starting a new out bag {" + name + "}");
				// Note: ettol el fog dobodni az Outok buffere, de ez nem baj, mert aminek el kellett mennie az mar elment
				startOutBag();
			} else {
				if (CFLConfig.vlog) LOG.info("Out.closeBag not starting a new out bag {" + name + "}");
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
		consumed = true;
		for(IN e: sp.buffers.get(i).elements) {
			op.pushInElement(e, logicalInputId);
		}
	}

	synchronized protected void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer outCFLSize = outCFLSizes.peek();

		assert latestCFL.get(outCFLSize - 1) == bbId;

		consumed = false;

		// Treat outputs
		for(Out o: outs) {
			o.startOutBag(outCFLSize);
		}

		// Tell the BagOperator that we are opening a new bag
		op.openOutBag();

		for (Input input: inputs) {
			//assert input.finishedSubpartitionCounter == -1;
			assert input.inputCFLSize == -1;

			for (InputSubpartition<IN> sp : input.inputSubpartitions) {
				sp.damming = true; // ezek kozul ugyebar nemelyiket majd false-ra allitja az activateLogicalInput mindjart
			}
		}

		chooseLogicalInputs(outCFLSize);
	}

	// Note: this activates all the logical inputs. (Cf. the override in PhiNode, which activates only one.)
	protected void chooseLogicalInputs(int outCFLSize) {
		// figure out the input bag IDs
		for (Input input: inputs) {
			int inputCFLSize;
			if (input.inputInSameBlock) {
				inputCFLSize = outCFLSize;
			} else {
				int i;
				for (i = outCFLSize - 2; input.bbId != latestCFL.get(i); i--) {}
				inputCFLSize = i + 1;
			}

			activateLogicalInput(input.id, outCFLSize, inputCFLSize);
		}
	}

	// Note: inputCFLSize should be set before this
	// Note: Also called from PhiNode
	void activateLogicalInput(int id, int outCFLSize, int inputCFLSize) {
		//  - For each subpartition, we tell it what to do:
		//    - Find a buffer that has the appropriate id
		//      - Give all the elements to the BagOperator
		//      - If it is the last one and not finished then remove the dam
		//    - If there is no appropriate buffer, then we do nothing for now
		//  - If we already have a notification that input bag is closed, then we tell this to the operator
		op.openInBag(id);
		Input input = inputs.get(id);
		assert input.inputCFLSize == -1;
		input.inputCFLSize = inputCFLSize;
		input.activeFor.add(outCFLSize);
		//assert input.currentBagID == null;
		input.currentBagID = new BagID(input.inputCFLSize, input.opID);
		for(InputSubpartition<IN> sp: input.inputSubpartitions) {
			int i;
			for(i = 0; i < sp.buffers.size(); i++) {
				if(sp.buffers.get(i).bagID.cflSize == input.inputCFLSize)
					break;
			}
			if(i < sp.buffers.size()) { // we have found an appropriate buffer
				assert input.currentBagID.equals(sp.buffers.get(i).bagID);
				giveBufferToBagOperator(sp, i, id);
				if(i == sp.buffers.size() - 1 && sp.status != InputSubpartition.Status.CLOSED) { // the last one and not finished
					sp.damming = false;
				}
			}
		}

		if (notifyCloseInputs.contains(input.currentBagID)) {
			input.closeCurrentInBag();
			if (notifyCloseInputEmpties.contains(input.currentBagID)) {
				consumed = true;
			}
		}

		// Asszem itt kell majd a remove buffer if bonyolult CFG-s condition
	}

	protected boolean updateOutCFLSizes(List<Integer> cfl) {
		if (cfl.get(cfl.size() - 1) == bbId) {
			outCFLSizes.add(cfl.size());
			return true;
		}
		return false;
	}

	private class MyCFLCallback implements CFLCallback {

		public void notify(List<Integer> cfl) {
			synchronized (BagOperatorHost.this) {
				latestCFL = cfl;

				if (CFLConfig.vlog) LOG.info("CFL notification: " + latestCFL + " {" + name + "}");

				// Note: figyelni kell, hogy itt hamarabb legyen az out-ok kezelese, mint a startOutBag hivas, mert az el fogja dobni a buffereket,
				// es van olyan, hogy ugyanannak a BB-nek az elerese mindket dolgot kivaltja

				for(Out o: outs) {
					o.notifyAppendToCFL(cfl);
				}

				boolean workInProgress = outCFLSizes.size() > 0;
				boolean hasAdded = updateOutCFLSizes(cfl);
				if (!workInProgress && hasAdded) {
					startOutBag();
				} else {
					if (CFLConfig.vlog) LOG.info("[" + name + "] CFLCallback.notify not starting an out bag, because outCFLSizes.size()=" + outCFLSizes.size());
				}
			}
		}

        @Override
        public void notifyTerminalBB() {
			LOG.info("CFL notifyTerminalBB");
			synchronized (BagOperatorHost.this) {
				terminalBBReached = true;
				if (outCFLSizes.isEmpty()) {
					cflMan.unsubscribe(cb);
				}
			}
        }

		@Override
		public void notifyCloseInput(BagID bagID, int opID) {
			synchronized (BagOperatorHost.this) {
				if (opID == BagOperatorHost.this.opID || opID == CFLManager.CloseInputBag.emptyBag) {
					assert !notifyCloseInputs.contains(bagID);
					notifyCloseInputs.add(bagID);

					if (opID == CFLManager.CloseInputBag.emptyBag) {
						notifyCloseInputEmpties.add(bagID);
					}

					for (Input inp : inputs) {
						//assert inp.currentBagID != null; // Ez kozben megsem lesz igaz, mert mostmar broadcastoljuk a closeInput-ot
						if (bagID.equals(inp.currentBagID)) {

							if (opID == CFLManager.CloseInputBag.emptyBag) {
								// Itt az EmptyFromEmpty marker interface azert nem kell, mert nem rontja ez el a dolgokat a consumed = true
								// akkor sem, ha nem empty lesz az eredmeny bag.
								consumed = true;
							}

							inp.closeCurrentInBag();
						}
					}
				}
			}
		}
	}

    // `normal` means not conditional.
	// If `normal` is false, then we set to damming until we reach its BB.
	// (This means that for example if targetBbId is the same as the operator's BbId, then we wait for the next iteration step.)
	public BagOperatorHost<IN, OUT> out(int splitId, int targetBbId, boolean normal, Partitioner<OUT> partitioner) {
		assert splitId == outs.size();
		outs.add(new Out((byte)splitId, targetBbId, normal, partitioner));
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
		private int targetBbId = -1;
		private boolean normal = false; // jelzi ha nem conditional
		private final Partitioner<OUT> partitioner;

		private ArrayList<OUT> buffer = null;
		private OutState state = OutState.IDLE;
		private int outCFLSize = -1; // The CFL that is being emitted. We need this, because cflSizes becomes empty when we become waiting.

		private boolean[] sentStart;

		Out(byte splitId, int targetBbId, boolean normal, Partitioner<OUT> partitioner) {
			this.splitId = splitId;
			this.targetBbId = targetBbId;
			this.normal = normal;
			this.partitioner = partitioner;
			this.sentStart = new boolean[partitioner.targetPara];
		}

		void collectElement(OUT e) {
			assert state == OutState.FORWARDING || state == OutState.DAMMING;
			if (state == OutState.FORWARDING) {
				sendElement(e);
			} else {
				buffer.add(e);
			}
		}

		void closeBag() {
			switch (state) {
				case IDLE:
					assert false;
					break;
				case DAMMING:
					state = OutState.WAITING;
					break;
				case WAITING:
					assert false;
					break;
				case FORWARDING:
					if (buffer != null) {
						for(OUT e: buffer) {
							sendElement(e);
						}
					}
					assert outCFLSize == outCFLSizes.peek();
					endBag();
					break;
			}
		}

		void startOutBag(Integer outCFLSize) {
			boolean targetReached = false;
			if (normal) {
				targetReached = true;
			} else {
				// Azert nem kell +1 az outCFLSize-nak, mert ugye az outCFL _utani_ elem igy is
				for (int i = outCFLSize; i < latestCFL.size(); i++) {
					if (latestCFL.get(i) == targetBbId) {
						targetReached = true;
					}
					if (latestCFL.get(i) == bbId) {
						break; // Merthogy akkor egy kesobbi bag folul fogja irni a mostanit.
					}
				}
			}
			this.outCFLSize = outCFLSize;
			if (!targetReached) {
				state = OutState.DAMMING;
				buffer = new ArrayList<>();
			} else {
				startBag();
				buffer = null; // Ez azert kell, mert vannak ilyen buffer != null checkek, es azok elkuldenek a regit
				state = OutState.FORWARDING;
			}
		}

		void notifyAppendToCFL(List<Integer> cfl) {
			if (!normal && (state == OutState.DAMMING || state == OutState.WAITING)) {
				if (cfl.get(cfl.size() - 1) == targetBbId) {
					// Leellenorizzuk, hogy nem irodik-e felul, mielott meg a jelenleg hozzaadottat elerne
					boolean overwritten = false;
					for (int i = outCFLSize; i < cfl.size() - 1; i++) {
						if (cfl.get(i) == bbId) {
							overwritten = true;
						}
					}
					if (!overwritten) {
						switch (state) {
							case IDLE:
								assert false; // Csak azert, mert a fentebbi if kizarja
								break;
							case DAMMING:
								assert outCFLSizes.size() > 0;
								assert outCFLSize == outCFLSizes.peek();
								startBag();
								state = OutState.FORWARDING;
								break;
							case WAITING:
								startBag();
								if (buffer != null) {
									for (OUT e : buffer) {
										sendElement(e);
									}
								}
								endBag();
								state = OutState.IDLE;
								break;
							case FORWARDING:
								assert false; // Csak azert, mert a fentebbi if kizarja
								break;
						}
					}
				}
			}
		}



		private void sendElement(OUT e) {
			short part = partitioner.getPart(e);
			// (Amugy ez a logika meg van duplazva a Bagify-ban is most)
			if (!sentStart[part]) {
				sendStart(part);
			}
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending element to " + part + ": " + new ElementOrEvent<>(subpartitionId, e, splitId, part));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, e, splitId, part), 0));
		}

		private void startBag() {
			for (int i=0; i<sentStart.length; i++)
				sentStart[i] = false;
		}

		private void endBag() {
			for (short i=0; i<sentStart.length; i++) {
				if (sentStart[i]) {
					sendEnd(i);
				}
			}
		}

		private void sendStart(short part) {
			sentStart[part] = true;
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, partitioner.targetPara, new BagID(outCFLSize, opID));
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending START to " + part + ": " + new ElementOrEvent<>(subpartitionId, event, splitId, part));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part), 0));
		}

		private void sendEnd(short part) {
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending END to " + part);
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, partitioner.targetPara, new BagID(outCFLSize, opID));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part), 0));
		}
	}

	// Logical input
	final class Input implements Serializable {

		int id; // marmint sorszam az inputs tombben
		int bbId;
		boolean inputInSameBlock; // marmint ugy ertve, hogy a kodban elotte (szoval ha utana van ugyanabban a blockban, akkor ez false)
		InputSubpartition<IN>[] inputSubpartitions;
		int inputCFLSize = -1; // always -1 when not working on an output bag or when we are not taking part in the computation of the current out bag (PhiNode)
		BagID currentBagID = null; // marmint ugy current, hogy amibol epp dolgozunk
		int opID = -1;
		Set<Integer> activeFor = new HashSet<>(); // outCFLSizes for which this Input is active

		Input(int id, int bbId, boolean inputInSameBlock, int opID) {
			this.id = id;
			this.bbId = bbId;
			this.inputInSameBlock = inputInSameBlock;
			this.opID = opID;
		}

		void closeCurrentInBag() {
			inputCFLSize = -1;
			//currentBagID = null; // ez itt rossz lenne, mert meg szuksegunk van ra kov hivasbol jovo dolgoknal
			op.closeInBag(id);
		}
	}

	private final static class InputSubpartition<T> {

		enum Status {OPEN, CLOSED}

		class Buffer {
			ArrayList<T> elements;
			BagID bagID;

			Buffer(BagID bagID) {
				this.elements = new ArrayList<>();
				this.bagID = bagID;
			}
		}

		ArrayList<Buffer> buffers;

		Status status;

		boolean damming;

		InputSubpartition() {
			buffers = new ArrayList<>();
			status = Status.CLOSED;
			damming = false;
		}

		void addNewBuffer(BagID bagID) {
			buffers.add(new Buffer(bagID));
		}
	}
}
