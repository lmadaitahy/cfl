package eu.stratosphere.labyrinth;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.Collections;
import java.util.List;

public class CondOutputSelector<T> implements OutputSelector<ElementOrEvent<T>> {

	private static final int maxSplit = 5;
	private static final List<String>[] cache = new List[maxSplit];

	static {
		for(Integer i=0; i<maxSplit; i++){
			cache[i] = Collections.singletonList(i.toString());
		}
	}

	@Override
	public Iterable<String> select(ElementOrEvent<T> elementOrEvent) {
		return cache[elementOrEvent.splitId];
	}
}
