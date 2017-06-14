package gg.jobs;

import gg.*;
import gg.operators.*;
import gg.partitioners2.RoundRobin;
import gg.util.LogicalInputIdFiller;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * // BB 0
 * i = 1
 * do {
 *     // BB 1
 *     i = i + 1
 * } while (i < 100)
 * // BB 2
 * assert i == 100
 */

public class SimpleCF {

	//private static final Logger LOG = LoggerFactory.getLogger(SimpleCF.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.getConfig().setParallelism(1);

		final int n = 100;

		final int bufferTimeout = 0;

		env.setBufferTimeout(bufferTimeout);

		CFLConfig.getInstance().terminalBBId = 2;
		env.addSource(new KickoffSource(0,1)).addSink(new DiscardingSink<>());
//		env.addSource(new KickoffSource(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
//				, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
//				, 1, 2)).addSink(new DiscardingSink<>());


		Integer[] input = new Integer[]{1};

		DataStream<ElementOrEvent<Integer>> inputBag0 =
				env.fromCollection(Arrays.asList(input))
						.transform("bagify",
								Util.tpe(), new Bagify<>(new RoundRobin<>(env.getParallelism())));

		DataStream<ElementOrEvent<Integer>> inputBag = inputBag0.map(new LogicalInputIdFiller<>(0));

		IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(1000000000);

		DataStream<ElementOrEvent<Integer>> phi = it
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("phi",inputBag.getType(),
//						new PhiNode<Integer>(1)
//								.addInput(0, 0)
//								.addInput(1, 1));
						new PhiNode2<Integer>(1)
								.addInput(0, 0, false)
								.addInput(1, 1, false)
								.out(0, 1, true));


		SplitStream<ElementOrEvent<Integer>> incedSplit = phi
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("inc-map",inputBag.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1)
								.addInput(0, 1, true)
								.out(0,1,false) // back edge
								.out(1,2,false) // out of the loop
								.out(2,1,true)) // to exit condition
				.split(new CondOutputSelector<>());

		DataStream<ElementOrEvent<Integer>> incedSplitL = incedSplit.select("0").map(new LogicalInputIdFiller<>(1));

		it.closeWith(incedSplitL);

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("smaller-than",Util.tpe(),
						new BagOperatorHost<>(
								new SmallerThan(n), 1)
								.addInput(0, 1, true)
								.out(0,1,true)).setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1)
								.addInput(0, 1, true)).setParallelism(1);

		// Edge going out of the loop
		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

		output.bt("Check i == " + n, Util.tpe(),
				new BagOperatorHost<>(
						new AssertEquals<>(n), 2)
						.addInput(0, 1, false)).setParallelism(1);

		output.addSink(new DiscardingSink<>());

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
