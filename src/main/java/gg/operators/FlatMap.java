package gg.operators;

public abstract class FlatMap<IN,OUT> extends BagOperator<IN,OUT> {

	@Override
	public void openOutBag() {
		super.openOutBag();
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		out.closeBag();
	}
}
