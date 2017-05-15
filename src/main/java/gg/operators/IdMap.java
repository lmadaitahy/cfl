package gg.operators;

public class IdMap<T> extends FlatMap<T,T> {
	@Override
	public void pushInElement(T e, int logicalInputId) {
		out.collectElement(e);
	}
}
