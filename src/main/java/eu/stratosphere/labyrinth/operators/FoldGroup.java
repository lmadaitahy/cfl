package eu.stratosphere.labyrinth.operators;

import java.util.HashMap;

public abstract class FoldGroup<K, IN, OUT> extends BagOperator<IN, OUT> {

	protected HashMap<K, OUT> hm;

	protected abstract K keyExtr(IN e);
}
