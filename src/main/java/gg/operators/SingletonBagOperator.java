package gg.operators;

import gg.BagOperatorOutputCollector;

/**
 * Verifies that input bags contain exactly one element.
 */
public abstract class SingletonBagOperator<IN, OUT> implements BagOperator<IN, OUT> {

	protected BagOperatorOutputCollector<OUT> collector;

	private int c = -1;

	@Override
	public void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		collector = out;
	}

	@Override
	public void OpenInBag() {
		c = 0;
	}

	@Override
	public void pushInElement(IN e) {
		c++;
	}

	@Override
	public void closeInBag() {
		assert c == 1; // Each of our input bags should contain exactly one element.  (ez elszurodhat ha pl. nem 1 a parallelismje egy SingletonBagOperatornak)
		c = -1;
		collector.closeBag();
	}
}
