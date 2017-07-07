package gg.jobs;

import gg.BagOperatorHost;
import gg.CFLConfig;
import gg.CondOutputSelector;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.MutableBagCC;
import gg.PhiNode;
import gg.operators.*;
import gg.partitioners.*;
import gg.util.LogicalInputIdFiller;
import gg.util.TupleIntInt;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels = toMutable(vertices.map(x => (x,x)), (x,y) => x)
// updates = labels
// do {
//     // BB 1
//     msgs = updates.join(edges).on(0).equalTo(0).with( ((vid, label), (u,v)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     updates = join(labels, minMsgs, (x,y) => x)
// 			.flatMap((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     update(labels, updates)
// } while (!updates.isEmpty)
// // BB 2
// toBag(labels).write
// assertBagEquals(labels_2, ...)


// SSA
// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels_0 = vertices.map(x => (x,x))
// mb = toMutable(labels_0)
// updates_0 = labels_0
// do {
//     // BB 1
//     updates_1 = phi(updates_0, updates_2)
//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     j = join(mb, minMsgs, (x,y) => x) // a kov. sor is beolvasztva most ebbe
//     updates_2 = j.flatMap( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     update(mb, updates_2)
//     nonEmpty = updates_2.nonEmpty
// } while (nonEmpty)
// // BB 2
// result = toBag(mb)
// result.write
// assertBagEquals(result, ...)


public class ConnectedComponentsMB {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectedComponentsMB.class);

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());
	public static TypeSerializer<TupleIntInt> tupleIntIntSer = TypeInformation.of(TupleIntInt.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (env instanceof LocalStreamEnvironment) {
			// mert kulonben az otthoni gepen 8 szal lenne, amikoris nem eleg a network buffer
			env.getConfig().setParallelism(4);
		}

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 32768); //16384
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(20, cfg); //20 // amugy sokszor 9-nel jon elo hiba, mivel ekkor mar nehol nem kap minden instance

		//env.getConfig().setParallelism(1); //////////

		int para = env.getParallelism();



		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());



		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		String outFile = ParameterTool.fromArgs(args).get("output");


		DataStream<TupleIntInt> edgesStream = Util.getGraphTuple2IntInt(env, args);

		DataStream<ElementOrEvent<TupleIntInt>> edges =
				edgesStream
						.transform("bagify",
								Util.tpe(), new Bagify<>(new TupleIntIntBy0(para), 14))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices0 = edges
				.bt("vertices0", Util.tpe(), new BagOperatorHost<>(new FlatMap<TupleIntInt, Integer>(){
			@Override
			public void pushInElement(TupleIntInt e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(e.f0);
			}
		}, 0, 0, tupleIntIntSer)
			.addInput(0,0,true,14)
			.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices = vertices0
				.bt("vertices", Util.tpe(),
				new BagOperatorHost<Integer, Integer>(new DistinctInt(), 0, 1, integerSer)
						.addInput(0,0,true, 0)
						.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<TupleIntInt>> labels_0 = vertices
				.bt("labels_0", Util.tpe(), new BagOperatorHost<>(new FlatMap<Integer,TupleIntInt>(){
			@Override
			public void pushInElement(Integer e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(TupleIntInt.of(e,e));
			}
		}, 0, 2, integerSer)
			.addInput(0,0,true,1)
			.out(0,0,true, new TupleIntIntBy0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());



		// ---------------- MutableBag ----------------

		IterativeStream<ElementOrEvent<TupleIntInt>> mbIt = labels_0
				.map(new LogicalInputIdFiller<>(0))
				.iterate(1000000000);

		DataStream<ElementOrEvent<TupleIntInt>> mbIt0 = mbIt.bt("MutableBagCC", Util.tpe(),
				new MutableBagCC(15)
			.addInput(0, 0, true, 2)
			.addInput(1, 1, true, 7)
			.addInput(2, 1, true, 15)
			.out(0,1,true, new TupleIntIntBy0(para)) // To update
			.out(1,1,true, new Always0<>(1)) // to nonEmpty
			.out(2,1,false, new Forward<>(para)) // back-edge to updates_1 phi-node
			.out(3, 2, true, outFile == null ? new Always0<>(1) : new Forward<>(para)) // output of toBag
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		SplitStream<ElementOrEvent<TupleIntInt>> mbSplit = mbIt0.split(new CondOutputSelector<>());




		DataStream<ElementOrEvent<TupleIntInt>> updates_0 = labels_0
				.bt("updates_0", Util.tpe(), new BagOperatorHost<>(new IdMap<TupleIntInt>(), 0, 3, tupleIntIntSer)
				.addInput(0,0,true,2)
				.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());


		// --- Iteration starts here ---

		IterativeStream<ElementOrEvent<TupleIntInt>> updatesIt = updates_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<TupleIntInt>> updates_1 = updatesIt
				.bt("updates_1", Util.tpe(), new PhiNode<TupleIntInt>(1, 5, tupleIntIntSer)
				.addInput(0,0,false,3)
				.addInput(1,1,false,15)
				.out(0,1,true, new TupleIntIntBy0(para))
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
		DataStream<ElementOrEvent<TupleIntInt>> msgs = edges
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_1.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>()) // Meg lehetne nezni, hogy enelkul is mukodik-e
				.bt("msgs", Util.tpe(), new BagOperatorHost<>(new JoinTupleIntInt(){
					@Override
					protected void udf(int b, TupleIntInt p) {
						out.collectElement(TupleIntInt.of(b, p.f1));
					}
				}, 1, 6, tupleIntIntSer)
				.addInput(0,0,false,14) // edges
				.addInput(1,1,true,5) // updates_1
				.out(0,1,true, new TupleIntIntBy0(para))
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//todo: combiner
		DataStream<ElementOrEvent<TupleIntInt>> minMsgs = msgs
				.bt("minMsgs", Util.tpe(), new BagOperatorHost<>(new GroupBy0Min1TupleIntInt(), 1, 7, tupleIntIntSer)
						.addInput(0,1,true,6)
						.out(0,1,true, new TupleIntIntBy0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		mbIt.closeWith(minMsgs.map(new LogicalInputIdFiller<>(1))
			.union(mbIt0.filter(new FilterFunction<ElementOrEvent<TupleIntInt>>() {
					@Override
					public boolean filter(ElementOrEvent<TupleIntInt> tuple2ElementOrEvent) throws Exception {
						return false;
					}
				}).returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
			));

		mbIt.closeWith(mbSplit.select("0").map(new LogicalInputIdFiller<>(2)));

		DataStream<ElementOrEvent<Boolean>> nonEmpty = mbSplit.select("1")
				.bt("nonEmpty",Util.tpe(),
						new BagOperatorHost<>(
								new NonEmpty<TupleIntInt>(), 1, 10, tupleIntIntSer)
								.addInput(0, 1, true, 15)
								.out(0,1,true, new Always0<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
				.setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = nonEmpty
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 11, booleanSer)
								.addInput(0, 1, true, 10))
				.setParallelism(1);

		updatesIt.closeWith(mbSplit.select("2").map(new LogicalInputIdFiller<>(1))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
			.union(updates_1.filter(new FilterFunction<ElementOrEvent<TupleIntInt>>() {
					@Override
					public boolean filter(ElementOrEvent<TupleIntInt> tuple2ElementOrEvent) throws Exception {
						return false;
					}
				}).returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
			));

		// --- Iteration ends here ---


		DataStream<ElementOrEvent<TupleIntInt>> result = mbSplit.select("3");

		if (outFile == null) {
			result.bt("AssertBagEquals", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<>(
					TupleIntInt.of(0, 0),
					TupleIntInt.of(1, 0),
					TupleIntInt.of(2, 0),
					TupleIntInt.of(3, 0),
					TupleIntInt.of(4, 0),
					TupleIntInt.of(5, 5),
					TupleIntInt.of(6, 5),
					TupleIntInt.of(7, 5)
					), 2, 13, tupleIntIntSer)
							.addInput(0, 1, true, 15)).setParallelism(1);
		} else {
			result.bt("GraphSink", Util.tpe(), new BagOperatorHost<>(new GraphSinkTupleIntInt(outFile), 2, 13, tupleIntIntSer)
							.addInput(0,1,true,15));
		}


		CFLConfig.getInstance().setNumToSubscribe();

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
