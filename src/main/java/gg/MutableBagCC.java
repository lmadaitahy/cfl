package gg;

import gg.operators.BagOperator;
import gg.partitioners.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

public class MutableBagCC extends BagOperatorHost<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private static final Logger LOG = LoggerFactory.getLogger(MutableBagCC.class);

	// Inputok:
	//  -1: semmi (ez a toBag-nel van)
	//  0: toMutable
	//  1: join
	//  2: update

	// Outok:
	//  0, 1, 2: a joinbol a harom kimeno
	//  3: toBag

	// These change together with outCFLSizes, and show which input/output to activate.
	private Queue<Integer> whichInput = new ArrayDeque<>();

	public MutableBagCC(int opID) {
		super(1, opID);
		op = new MutableBagOperator();
	}

	@Override
	protected boolean updateOutCFLSizes(List<Integer> cfl) {
		int addedBB = cfl.get(cfl.size() - 1);
		outCFLSizes.add(cfl.size()); // mert minden BB-ben van egy muvelet
		if (addedBB == 1) { // mert BB 1-ben ket muvelet is van ra
			outCFLSizes.add(cfl.size());
		}
		switch (addedBB) {
			case 0:
				whichInput.add(0);
				break;
			case 1:
				whichInput.add(1);
				whichInput.add(2);
				break;
			case 2:
				whichInput.add(-1);
				break;
			default:
				assert false;
		}
		return true;
	}

	@Override
	protected void outCFLSizesRemove() {
		super.outCFLSizesRemove(); // itt kell ez a super hivas
		whichInput.remove();
	}

	@Override
	protected void chooseLogicalInputs(int outCFLSize) {
		int inpID = whichInput.peek();
		assert ((MutableBagOperator)op).inpID == inpID;
		if (inpID != -1) {
			assert inputs.get(inpID).inputInSameBlock;
			activateLogicalInput(inpID, outCFLSize, outCFLSize);
		}
	}

	@Override
	protected void chooseOuts() {
		for (Out out: outs) {
			out.active = false;
		}

		int inID = whichInput.peek();
		switch (inID) {
			case -1: // toBag
				outs.get(3).active = true;
				break;
			case 0: // toMutable
				break;
			case 1: // Join
				outs.get(0).active = true;
				outs.get(1).active = true;
				outs.get(2).active = true;
				break;
			case 2: // update
				break;
			default:
				assert false;
				break;
		}
	}

	class MutableBagOperator extends BagOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		HashMap<Integer, Tuple2<Integer, Integer>> hm = new HashMap<>();

		int inpID = -3;

		@Override
		public void openOutBag() {
			super.openOutBag();

			int inpID = whichInput.peek();
			((MutableBagOperator)op).inpID = inpID;

			switch (inpID) {
				case -1:
					for (HashMap.Entry<Integer, Tuple2<Integer, Integer>> e: hm.entrySet()) {
						out.collectElement(e.getValue());
					}
					out.closeBag();
					break;
				case 0:
					hm.clear();
					break;
				case 1:
					// nothing to do here
					break;
				case 2:
					// nothing to do here
					break;
			}
		}

		@Override
		public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
			super.pushInElement(e, logicalInputId);
			switch (logicalInputId) {
				case 0: // toMutable
					assert inpID == 0;
					hm.put(e.f0, e);
					break;
				case 1: // join
					{
						assert inpID == 1;
						Tuple2<Integer, Integer> g = hm.get(e.f0);
						assert g != null; // az altalanos interface-nel nem, de a CC-nel mindig benne kell lennie
						if (g.f1 > e.f1) {
							out.collectElement(e);
						}
						break;
					}
				case 2: // update
					assert inpID == 2;
					Tuple2<Integer, Integer> present = hm.replace(e.f0, e);
					assert present != null; // az altalanos interface-nel nem, de a CC-nel mindig benne kell lennie
					break;
				default:
					assert false;
					break;
			}
		}

		@Override
		public void closeInBag(int inputId) {
			super.closeInBag(inputId);
			out.closeBag();
		}
	}
}
