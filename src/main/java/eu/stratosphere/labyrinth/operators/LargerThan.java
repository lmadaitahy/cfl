package eu.stratosphere.labyrinth.operators;

import java.io.Serializable;

public class LargerThan extends SingletonBagOperator<Double,Boolean> implements Serializable {

	private final double x;

	public LargerThan(double x) {
		this.x = x;
	}

	@Override
	public void pushInElement(Double e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		out.collectElement(e > x);
	}
}
