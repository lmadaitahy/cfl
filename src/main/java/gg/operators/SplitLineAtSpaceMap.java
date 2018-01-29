package gg.operators;

public class SplitLineAtSpaceMap extends FlatMap<String, String> {
	@Override
	public void pushInElement(String e, int LogicalInputID) {
		for (String elem : e.split(" ")) { out.collectElement(elem); }
	}
}
