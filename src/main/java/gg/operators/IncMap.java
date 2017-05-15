package gg.operators;

public class IncMap extends FlatMap<Integer,Integer> {
	@Override
	public void pushInElement(Integer e, int logicalInputId) {
		System.out.println("incing");
		out.collectElement(e + 1);
	}
}
