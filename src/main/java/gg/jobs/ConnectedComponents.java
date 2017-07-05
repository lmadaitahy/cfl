package gg.jobs;

import gg.*;
import gg.operators.*;
import gg.partitioners.Always0;
import gg.partitioners.Tuple2by0;
import gg.partitioners.FlinkPartitioner;
import gg.partitioners.Forward;
import gg.util.LogicalInputIdFiller;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels = vertices.map(x => (x,x))
// updates = labels
// do {
//     // BB 1
//     msgs = updates.join(edges).on(0).equalTo(0).with( ((vid, label), (u,v)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     updates = labels.join(minMsgs).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     labels = labels.update(updates)
// } while (!updates.isEmpty)
// // BB 2
// labels.write
// assertBagEquals(labels_2, ...)


// SSA
// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels_0 = vertices.map(x => (x,x))
// updates_0 = labels_0
// do {
//     // BB 1
//     labels_1 = phi(labels_0, labels_2)
//     updates_1 = phi(updates_0, updates_2)
//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     labels_2 = labels_1.update(updates_2)
//     nonEmpty = updates_2.nonEmpty
// } while (nonEmpty)
// // BB 2
// labels_2.write
// assertBagEquals(labels_2, ...)


public class ConnectedComponents {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectedComponents.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (env instanceof LocalStreamEnvironment) {
			// mert kulonben az otthoni gepen 8 szal lenne, amikoris nem eleg a network buffer
			env.getConfig().setParallelism(4);
		}

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 32768); //16384
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(20, cfg); //20 // amugy sokszor 9-nel jon elo hiba, mivel ekkor mar nehol nem kap minden instance

		//env.getConfig().setParallelism(1);

		int para = env.getParallelism();



		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		String outFile = ParameterTool.fromArgs(args).get("output");


		DataStream<Tuple2<Integer, Integer>> edgesStream = Util.getGraph(env, args);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> edges =
				edgesStream
						.transform("bagify",
								Util.tpe(), new Bagify<>(new Tuple2by0(para), 14))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices0 = edges
				.bt("vertices0", Util.tpe(), new BagOperatorHost<>(new FlatMap<Tuple2<Integer, Integer>, Integer>(){
			@Override
			public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(e.f0);
			}
		}, 0, 0)
			.addInput(0,0,true,14)
			.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices = vertices0
				.bt("vertices", Util.tpe(),
				new BagOperatorHost<Integer, Integer>(new Distinct<>(), 0, 1)
						.addInput(0,0,true, 0)
						.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_0 = vertices
				.bt("labels_0", Util.tpe(), new BagOperatorHost<>(new FlatMap<Integer,Tuple2<Integer,Integer>>(){
			@Override
			public void pushInElement(Integer e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(Tuple2.of(e,e));
			}
		}, 0, 2)
			.addInput(0,0,true,1)
			.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_0 = labels_0
				.bt("updates_0", Util.tpe(), new BagOperatorHost<>(new IdMap<Tuple2<Integer, Integer>>(), 0, 3)
				.addInput(0,0,true,2)
				.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());


		// --- Iteration starts here ---

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> labelsIt = labels_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_1 = labelsIt
				.bt("labels_1", Util.tpe(), new PhiNode<Tuple2<Integer, Integer>>(1, 4)
				.addInput(0,0,false,2)
				.addInput(1,1,false,9)
				.out(0,1,true, new Tuple2by0(para)) // (this goes to two places, but none of them is conditional)
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> updatesIt = updates_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_1 = updatesIt
				.bt("updates_1", Util.tpe(), new PhiNode<Tuple2<Integer, Integer>>(1, 5)
				.addInput(0,0,false,3)
				.addInput(1,1,false,8)
				.out(0,1,true, new Tuple2by0(para))
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> msgs = edges
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_1.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>()) // Meg lehetne nezni, hogy enelkul is mukodik-e
				.bt("msgs", Util.tpe(), new BagOperatorHost<>(new Join(){
					@Override
					protected void udf(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
						out.collectElement(Tuple2.of(a.f1, b.f1));
					}
				}, 1, 6)
				.addInput(0,0,false,14) // edges
				.addInput(1,1,true,5) // updates_1
				.out(0,1,true, new Tuple2by0(para))
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//todo: combiner
		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> minMsgs = msgs
				.bt("minMsgs", Util.tpe(), new BagOperatorHost<>(new GroupBy0Min1(), 1, 7)
						.addInput(0,1,true,6)
						.out(0,1,true, new Tuple2by0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(minMsgs.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>())
				.bt("updates_2", Util.tpe(), new BagOperatorHost<>(new Join(){
					@Override
					protected void udf(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
						if (a.f1 > b.f1) {
							out.collectElement(b);
						}
					}
				}, 1, 8)
								.addInput(0,1,true,4)
								.addInput(1,1,true,7)
								.out(0,1,true, new Tuple2by0(para)) // To labels_2 update
								.out(1,1,true, new Always0<>(1)) // to nonEmpty
						        .out(2,1,false, new Forward<>(para)) // back-edge
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		//     labels_2 = labels_1.update(updates_2)
		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_2.select("0").map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>())
				.bt("labels_2",Util.tpe(),new BagOperatorHost<>(new UpdateJoin(),1,9)
								.addInput(0,1,true,4) // labels_1
								.addInput(1,1,true,8) // updates_2
								.out(0,1,false, new Forward<>(para)) // back-edge
								.out(1,2,false, outFile == null ? new Always0<>(1) : new Forward<>(para)) // out of the loop
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		labelsIt.closeWith(labels_2.select("0").map(new LogicalInputIdFiller<>(1)));
		updatesIt.closeWith(updates_2.select("2").map(new LogicalInputIdFiller<>(1)));

		// exit cond

		DataStream<ElementOrEvent<Boolean>> nonEmpty = updates_2.select("1")
				.bt("nonEmpty",Util.tpe(),
						new BagOperatorHost<>(
								new NonEmpty<Tuple2<Integer, Integer>>(), 1, 10)
								.addInput(0, 1, true, 8)
								.out(0,1,true, new Always0<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
				.setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = nonEmpty
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 11)
								.addInput(0, 1, true, 10))
				.setParallelism(1);

		// --- Iteration ends here ---


		if (outFile == null) {
			labels_2.select("1").bt("AssertBagEquals", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<>(
					Tuple2.of(0, 0),
					Tuple2.of(1, 0),
					Tuple2.of(2, 0),
					Tuple2.of(3, 0),
					Tuple2.of(4, 0),
					Tuple2.of(5, 5),
					Tuple2.of(6, 5),
					Tuple2.of(7, 5)
			), 2, 13)
					.addInput(0, 1, false, 9)).setParallelism(1);
		} else {
			labels_2.select("1").bt("GraphSink", Util.tpe(), new BagOperatorHost<>(new GraphSink(outFile), 2, 13)
					.addInput(0,1,false,9));
		}

		CFLConfig.getInstance().setNumToSubscribe();

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
