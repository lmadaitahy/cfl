package gg.operators;

import gg.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ConditionNode extends SingletonBagOperator<Boolean, Unit> implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(ConditionNode.class);

	private final int trueBranchBbId;
	private final int falseBranchBbId;

	public ConditionNode(int trueBranchBbId, int falseBranchBbId) { //todo: ezeknek ugyebar tomboknek kene lenniuk
		this.trueBranchBbId = trueBranchBbId;
		this.falseBranchBbId = falseBranchBbId;
	}

	@Override
	public void pushInElement(Boolean e) {
		super.pushInElement(e);
		System.out.println("ConditionNode(" + e + ")");
		collector.appendToCfl(e ? trueBranchBbId : falseBranchBbId);
	}
}
