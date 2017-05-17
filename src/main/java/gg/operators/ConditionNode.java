package gg.operators;

import gg.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ConditionNode extends SingletonBagOperator<Boolean, Unit> {

	protected static final Logger LOG = LoggerFactory.getLogger(ConditionNode.class);

	private final int trueBranchBbId;
	private final int falseBranchBbId;

	public ConditionNode(int trueBranchBbId, int falseBranchBbId) { //todo: ideally, these would be arrays
		this.trueBranchBbId = trueBranchBbId;
		this.falseBranchBbId = falseBranchBbId;
	}

	@Override
	public void pushInElement(Boolean e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		out.appendToCfl(e ? trueBranchBbId : falseBranchBbId);
	}
}
