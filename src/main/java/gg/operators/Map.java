package gg.operators;

import gg.BagOperatorOutputCollector;

import java.io.Serializable;

public abstract class Map<IN,OUT> implements BagOperator<IN,OUT>, Serializable {

	BagOperatorOutputCollector<OUT> out;

	@Override
	public void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		this.out = out;
	}

	@Override
	public void OpenInBag() {
		System.out.println("BagMap OpenInBag");
	}

	@Override
	public void closeInBag() {
		System.out.println("BagMap closeInBag");
		out.closeBag();
	}
}
