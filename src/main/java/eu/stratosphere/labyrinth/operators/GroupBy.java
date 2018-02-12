package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GroupBy<LEFT,RIGHT> extends BagOperator<Tuple2<LEFT,RIGHT>, Tuple2<LEFT,RIGHT>> {
	protected HashMap<LEFT,RIGHT> hm;
}
