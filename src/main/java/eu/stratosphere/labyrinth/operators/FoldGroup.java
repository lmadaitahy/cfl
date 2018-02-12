package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public abstract class FoldGroup<K, A, B> extends BagOperator<Tuple2<K, A>, Tuple2<K, B>> {

	protected HashMap<K, B> hm;
}
