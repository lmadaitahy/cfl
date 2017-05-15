package gg.operators;

public abstract class FlatMap<IN,OUT> extends BagOperator<IN,OUT> {

	@Override
	public void openOutBag() {
		super.openOutBag();
		System.out.println("BagMap openOutBag");
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		System.out.println("BagMap closeInBag");
		out.closeBag();
	}
}
