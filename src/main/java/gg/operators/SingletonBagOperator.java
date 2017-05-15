package gg.operators;

/**
 * Verifies that input bags contain exactly one element.
 */
public abstract class SingletonBagOperator<IN, OUT> extends BagOperator<IN, OUT> {

	private static final int closedC = -1000000;

	private int c = closedC;

	@Override
	public void openOutBag() {
		super.openOutBag();
		assert c == closedC;
		c = 0;
	}

	@Override
	public void pushInElement(IN e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		c++;
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		assert c == 1; // Each of our input bags should contain exactly one element.  (ez elszurodhat ha pl. nem 1 a parallelismje egy SingletonBagOperatornak)
		c = closedC;
		out.closeBag();
	}
}
