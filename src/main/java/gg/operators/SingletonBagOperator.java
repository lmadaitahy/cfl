package gg.operators;

import gg.BagOperatorOutputCollector;

/**
 * Verifies that input bags contain exactly one element.
 */
public abstract class SingletonBagOperator<IN, OUT> implements BagOperator<IN, OUT> {

	private static final int closedC = -1000000;

	protected BagOperatorOutputCollector<OUT> collector;

	private int c = closedC;

	@Override
	public void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		collector = out;
	}

	@Override
	public void OpenInBag() {
		assert c == closedC;
		c = 0;
	}

	@Override
	public void pushInElement(IN e) {
		c++;
	}

	@Override
	public void closeInBag(int inputId) {
		assert c == 1; // Each of our input bags should contain exactly one element.  (ez elszurodhat ha pl. nem 1 a parallelismje egy SingletonBagOperatornak)
		c = closedC;
		collector.closeBag();
	}
}
