package gg.jobs;

import gg.BagOperatorHost;
import gg.CFLManager;
import gg.CondOutputSelector;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.operators.ConditionNode;
import gg.operators.IdMap;
import gg.operators.Bagify;
import gg.operators.IncMap;
import gg.operators.SmallerThan;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import scala.xml.Elem;

import java.util.Arrays;

/**
 * i = 1
 * do {
 *     i = i + 1
 * } while (i < 10)
 * print(i)
 */

public class CFLProbaSimpleCF {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);

		env.addSource(new KickoffSource(0,1)).addSink(new DiscardingSink<>());

		Integer[] input = new Integer[]{1};

		DataStream<ElementOrEvent<Integer>> inputBag =
				env.fromCollection(Arrays.asList(input))
						.transform("bagify",
								Util.tpe(), new Bagify<>());

		IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(1000000000);

		SplitStream<ElementOrEvent<Integer>> incedSplit = it
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("inc-map",inputBag.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1, new Integer[]{0,1}, false)
								.out(0,1,false) // back edge
								.out(1,2,false) // out of the loop
								.out(2,1,true)) // to exit condition
				.split(new CondOutputSelector<>());

		it.closeWith(incedSplit.select("0"));

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("smaller-than",Util.tpe(),
						new BagOperatorHost<>(
								new SmallerThan(10), 1, new Integer[]{1}, true)
								.out(0,1,true));

		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<Boolean, Unit>(
								new ConditionNode(1,2), 1, new Integer[]{1}, true));

		// Edge going out of the loop
		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

		output.print();
		env.execute();
	}
}
