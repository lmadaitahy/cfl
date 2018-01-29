package gg.operators;

import org.apache.flink.api.java.tuple.Tuple2;

public class WordToWord1TupleMap extends FlatMap<String, Tuple2<String, Integer>> {
	@Override
	public void pushInElement(String e, int LogicalInput) { out.collectElement(new Tuple2<>(e, 1)); }
}
