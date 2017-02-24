package gg.jobs;

import gg.*;
import gg.operators.ConditionNode;
import gg.operators.IdMap;
import gg.operators.Bagify;
import gg.operators.IncMap;
import gg.operators.SmallerThan;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.functions.MapFunction;
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
		//env.getConfig().setParallelism(1);

		final int bufferTimeout = 0;

		env.setBufferTimeout(bufferTimeout); // ez a lenyeg!

		env.addSource(new KickoffSource(0,1)).addSink(new DiscardingSink<>());
//		env.addSource(new KickoffSource(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
//				, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
//				, 1, 2)).addSink(new DiscardingSink<>());



		Integer[] input = new Integer[]{1};

		DataStream<ElementOrEvent<Integer>> inputBag0 =
				env.fromCollection(Arrays.asList(input))
						.transform("bagify",
								Util.tpe(), new Bagify<>());

		DataStream<ElementOrEvent<Integer>> inputBag = inputBag0.map(new MapFunction<ElementOrEvent<Integer>, ElementOrEvent<Integer>>() {
			@Override
			public ElementOrEvent<Integer> map(ElementOrEvent<Integer> e) throws Exception {
				e.logicalInputId = 0;
				return e;
			}
		}).setBufferTimeout(bufferTimeout);

		IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(1000000000);
		it.setBufferTimeout(bufferTimeout);

		DataStream<ElementOrEvent<Integer>> phi = it
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("phi",inputBag.getType(),
						new PhiNode<Integer>(1)
								.addInput(0, 0)
								.addInput(1, 1)).setBufferTimeout(bufferTimeout);


		SplitStream<ElementOrEvent<Integer>> incedSplit = phi
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("inc-map",inputBag.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1, 1, true)
								.out(0,1,false) // back edge
								.out(1,2,false) // out of the loop
								.out(2,1,true)) // to exit condition
				.setBufferTimeout(bufferTimeout).split(new CondOutputSelector<>());

		DataStream<ElementOrEvent<Integer>> incedSplitL = incedSplit.select("0").map(new MapFunction<ElementOrEvent<Integer>, ElementOrEvent<Integer>>() {
			@Override
			public ElementOrEvent<Integer> map(ElementOrEvent<Integer> e) throws Exception {
				e.logicalInputId = 1;
				return e;
			}
		}).setBufferTimeout(bufferTimeout);

		it.closeWith(incedSplitL);

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("smaller-than",Util.tpe(),
						new BagOperatorHost<>(
								new SmallerThan(100), 1, 1, true)
								.out(0,1,true)).setParallelism(1).setBufferTimeout(bufferTimeout);

		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 1, true)).setParallelism(1).setBufferTimeout(bufferTimeout);

		// Edge going out of the loop
		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

		output.print();

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
