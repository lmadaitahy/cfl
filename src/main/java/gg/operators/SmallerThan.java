package gg.operators;

import java.io.Serializable;

public class SmallerThan extends SingletonBagOperator<Integer,Boolean> implements Serializable {

	private int x;

	public SmallerThan(int x) {
		this.x = x;
	}

	@Override
	public void pushInElement(Integer e) {
		super.pushInElement(e);
		System.out.println("SmallerThaning");
		collector.collectElement(e < x);
	}
}
