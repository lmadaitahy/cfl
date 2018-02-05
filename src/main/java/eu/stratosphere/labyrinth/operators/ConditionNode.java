package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionNode extends SingletonBagOperator<Boolean, Unit> {

	protected static final Logger LOG = LoggerFactory.getLogger(ConditionNode.class);

	private final int[] trueBranchBbIds;
	private final int[] falseBranchBbIds;
	
	public ConditionNode(int trueBranchBbId, int falseBranchBbId) {
		this(new int[]{trueBranchBbId}, new int[]{falseBranchBbId});
	}

	public ConditionNode(int[] trueBranchBbIds, int[] falseBranchBbIds) {
		this.trueBranchBbIds = trueBranchBbIds;
		this.falseBranchBbIds = falseBranchBbIds;
	}

	@Override
	public void pushInElement(Boolean e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		for (int b: e ? trueBranchBbIds : falseBranchBbIds) {
			out.appendToCfl(b);
		}
	}
}
