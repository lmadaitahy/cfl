package eu.stratosphere.labyrinth.jobs;

import eu.stratosphere.labyrinth.operators.IncMap;
import eu.stratosphere.labyrinth.CFLConfig;
import eu.stratosphere.labyrinth.ElementOrEvent;
import eu.stratosphere.labyrinth.KickoffSource;
import eu.stratosphere.labyrinth.LabyNode;
import eu.stratosphere.labyrinth.LabySource;
import eu.stratosphere.labyrinth.operators.AssertEquals;
import eu.stratosphere.labyrinth.operators.ConditionNode;
import eu.stratosphere.labyrinth.operators.SmallerThan;
import eu.stratosphere.labyrinth.partitioners.Always0;
import eu.stratosphere.labyrinth.partitioners.Random;
import eu.stratosphere.labyrinth.util.TupleIntInt;
import eu.stratosphere.labyrinth.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

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

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(40, cfg);

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);

		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
		PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		//KickoffSource kickoffSrc = new KickoffSource(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		Integer[] input = new Integer[]{1};

//		DataStream<ElementOrEvent<Integer>> inputBag0 =
//				env.fromCollection(Arrays.asList(input))
//						.transform("bagify",
//								Util.tpe(), new Bagify<>(new RoundRobin<>(env.getParallelism()), 0))
//						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
//						.setConnectionType(new FlinkPartitioner<>());

		LabySource<Integer> inputBag = new LabySource<>(env.fromCollection(Arrays.asList(input)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}));

		//IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(1000000000);

//		DataStream<ElementOrEvent<Integer>> phi = it
//				//.setConnectionType(new Random<>())
//				.bt("phi",inputBag.getType(),
//						new PhiNode<Integer>(1, 1, integerSer)
//								.addInput(0, 0, false, 0)
//								.addInput(1, 1, false, 2)
//								.out(0, 1, true, new Random<>(env.getParallelism())))
//				.setConnectionType(new FlinkPartitioner<>());

		LabyNode<Integer, Integer> phi =
				LabyNode.phi("phi", 1, new Random<>(env.getParallelism()), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.addInput(inputBag, false);


//		SplitStream<ElementOrEvent<Integer>> incedSplit = phi
//				//.setConnectionType(new Random<>())
//				.bt("inc-map",inputBag.getType(),
//						new BagOperatorHost<>(
//								new IncMap(), 1, 2, integerSer)
//								.addInput(0, 1, true, 1)
//								.out(0,1,false, new Random<>(env.getParallelism())) // back edge
//								.out(1,2,false, new Random<>(1)) // out of the loop
//								.out(2,1,true, new Random<>(1))) // to exit condition
//				.setConnectionType(new FlinkPartitioner<>())
//				.split(new CondOutputSelector<>());

//		DataStream<ElementOrEvent<Integer>> incedSplitL = incedSplit.select("0").map(new LogicalInputIdFiller<>(1));
//
//		it.closeWith(incedSplitL);

		LabyNode<Integer, Integer> inced =
				new LabyNode<>("inc-map", new IncMap(), 1, new Random<>(env.getParallelism()), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.addInput(phi, true, false);

		phi.addInput(inced, false, true);

//		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
//				//.setConnectionType(new Random<>())
//				.bt("smaller-than",Util.tpe(),
//						new BagOperatorHost<>(
//								new SmallerThan(n), 1, 3, integerSer)
//								.addInput(0, 1, true, 2)
//								.out(0,1,true, new Random<>(1)))
//				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
//				.setParallelism(1);
//				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert 1->1

		LabyNode<Integer, Boolean> smallerThan =
				new LabyNode<>("smaller-than", new SmallerThan(n), 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
						.addInput(inced, true, false)
						.setParallelism(1);

//		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
//				.bt("exit-cond",Util.tpe(),
//						new BagOperatorHost<>(
//								new ConditionNode(1,2), 1, 4, booleanSer)
//								.addInput(0, 1, true, 3)).setParallelism(1);
//				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		LabyNode<Boolean, Unit> exitCond =
				new LabyNode<>("exit-cond", new ConditionNode(1,2), 1, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
						.addInput(smallerThan, true, false)
						.setParallelism(1);

//		// Edge going out of the loop
//		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

//		output.bt("Check i == " + n, Util.tpe(),
//				new BagOperatorHost<>(
//						new AssertEquals<>(n), 2, 5, integerSer)
//						.addInput(0, 1, false, 2)).setParallelism(1);
//				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		LabyNode<Integer, Unit> assertEquals =
				new LabyNode<>("Check i == " + n, new AssertEquals<>(n), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
					.addInput(inced, false, true)
					.setParallelism(1);


		//output.addSink(new DiscardingSink<>());

//		CFLConfig.getInstance().setNumToSubscribe();

		LabyNode.translateAll();

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
