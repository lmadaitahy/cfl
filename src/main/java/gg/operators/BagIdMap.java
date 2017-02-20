package gg.operators;

public class BagIdMap<T> extends BagMap<T,T> {
	@Override
	public void pushInElement(T e) {
		out.collectElement(e);
	}
}
