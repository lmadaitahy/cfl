package eu.stratosphere.labyrinth.operators;

public class IdMap<T> extends FlatMap<T,T> {
	@Override
	public void pushInElement(T e, int logicalInputId) {
		super.pushInElement(e,logicalInputId);
		out.collectElement(e);
	}
}
