package gg.operators;

public class IdMap<T> extends Map<T,T> {
	@Override
	public void pushInElement(T e) {
		out.collectElement(e);
	}
}
