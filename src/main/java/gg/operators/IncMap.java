package gg.operators;

public class IncMap extends Map<Integer,Integer> {
	@Override
	public void pushInElement(Integer e) {
		System.out.println("incing");
		out.collectElement(e + 1);
	}
}
