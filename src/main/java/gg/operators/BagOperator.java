package gg.operators;

import gg.BagOperatorOutputCollector;

import java.io.Serializable;

public abstract class BagOperator<IN, OUT> implements Serializable {

	protected BagOperatorOutputCollector<OUT> out;

	private boolean[] open = new boolean[]{false, false};

	public void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		this.out = out;
	}

	public void openOutBag() {}

	public final void openInBag(int logicalInputId) {
		assert !open[logicalInputId];
		open[logicalInputId] = true;
	}

	public void pushInElement(IN e, int logicalInputId) {
		assert open[logicalInputId];
	}

	public void closeInBag(int inputId) {
		assert open[inputId];
		open[inputId] = false;
	}

}
