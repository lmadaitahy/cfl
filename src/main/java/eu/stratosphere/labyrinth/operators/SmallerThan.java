package eu.stratosphere.labyrinth.operators;

import java.io.Serializable;

public class SmallerThan extends SingletonBagOperator<Integer,Boolean> implements Serializable {

	private final int x;

	public SmallerThan(int x) {
		this.x = x;
	}

	@Override
	public void pushInElement(Integer e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		out.collectElement(e < x);
	}
}
