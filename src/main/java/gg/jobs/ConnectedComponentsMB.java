package gg.jobs;

import gg.BagOperatorHost;
import gg.CFLConfig;
import gg.CondOutputSelector;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.MutableBagCC;
import gg.PhiNode;
import gg.operators.AssertBagEquals;
import gg.operators.Bagify;
import gg.operators.ConditionNode;
import gg.operators.Distinct;
import gg.operators.FlatMap;
import gg.operators.GroupBy0Min1;
import gg.operators.IdMap;
import gg.operators.Join;
import gg.operators.NonEmpty;
import gg.operators.Print;
import gg.operators.UpdateJoin;
import gg.partitioners.FlinkPartitioner;
import gg.partitioners.Forward;
import gg.partitioners.Tuple2by0;
import gg.util.LogicalInputIdFiller;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Elem;

import java.util.Arrays;


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

		//env.setBufferTimeout(0);

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

		@SuppressWarnings("unchecked")
		//Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{Tuple2.of(0,1)};
		Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{
				Tuple2.of(0,1),
				Tuple2.of(1,2),
				Tuple2.of(3,4),
				Tuple2.of(4,0),
				Tuple2.of(5,6),
				Tuple2.of(5,7)
		};

		// berakjuk megforditva is az eleket
		@SuppressWarnings("unchecked")
		Tuple2<Integer, Integer>[] edgesNB = new Tuple2[edgesNB0.length * 2];
		for (int i=0; i<edgesNB0.length; i++) {
			edgesNB[i*2] = edgesNB0[i];
			edgesNB[i*2+1] = Tuple2.of(edgesNB0[i].f1, edgesNB0[i].f0);
		}


		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> edges =
				env.fromCollection(Arrays.asList(edgesNB))
						.transform("bagify",
								Util.tpe(), new Bagify<>(new Tuple2by0(para), 14))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices0 = edges
				//.setConnectionType(new gg.partitioners.Forward<>())
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
				//.setConnectionType(new gg.partitioners.Forward<>())
				.bt("vertices", Util.tpe(),
				new BagOperatorHost<Integer, Integer>(new Distinct<>(), 0, 1)
						.addInput(0,0,true, 0)
						.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_0 = vertices
				//.setConnectionType(new gg.partitioners.Forward<>())
				.bt("labels_0", Util.tpe(), new BagOperatorHost<>(new FlatMap<Integer,Tuple2<Integer,Integer>>(){
			@Override
			public void pushInElement(Integer e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(Tuple2.of(e,e));
			}
		}, 0, 2)
			.addInput(0,0,true,1)
			.out(0,0,true, new Tuple2by0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());



		// ---------------- MutableBag ----------------

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> mbIt = labels_0
				.map(new LogicalInputIdFiller<>(0))
				.iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> mbIt0 = mbIt.bt("MutableBagCC", Util.tpe(),
				new MutableBagCC(15)
			.addInput(0, 0, true, 2)
			.addInput(1, 1, true, 7)
			.addInput(2, 1, true, 15)
			.out(0,1,true, new Tuple2by0(para)) // To update
			.out(1,1,true, new Forward<>(1)) // to nonEmpty
			.out(2,1,false, new Forward<>(para)) // back-edge to updates_1 phi-node
			.out(3, 2, true, new Forward<>(1)) // output of toBag  // majd itt a para-t at kell allitani, amikor nagy adaton futtatok jobot!
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> mbSplit = mbIt0.split(new CondOutputSelector<>());




		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_0 = labels_0
				//.setConnectionType(new gg.partitioners.Forward<>())
				.bt("updates_0", Util.tpe(), new BagOperatorHost<>(new IdMap<Tuple2<Integer, Integer>>(), 0, 3)
				.addInput(0,0,true,2)
				.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());


		// --- Iteration starts here ---

//		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> labelsIt = labels_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

//		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_1 = labelsIt
//				//.setConnectionType(new gg.partitioners.Forward<>())
//				.bt("labels_1", Util.tpe(), new PhiNode<Tuple2<Integer, Integer>>(1, 4)
//				.addInput(0,0,false,2)
//				.addInput(1,1,false,9)
//				.out(0,1,true, new Tuple2by0(para)) // (this goes to two places, but none of them is conditional)
//		)
//				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
//				.setConnectionType(new FlinkPartitioner<>());

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> updatesIt = updates_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_1 = updatesIt
				//.setConnectionType(new gg.partitioners.Forward<>())
				.bt("updates_1", Util.tpe(), new PhiNode<Tuple2<Integer, Integer>>(1, 5)
				.addInput(0,0,false,3)
				.addInput(1,1,false,15)
				.out(0,1,true, new Tuple2by0(para))
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> msgs = edges
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_1.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>()) // Meg lehetne nezni, hogy enelkul is mukodik-e
				//.setConnectionType(new Tuple2by0())
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
				//.setConnectionType(new Tuple2by0())
				.bt("minMsgs", Util.tpe(), new BagOperatorHost<>(new GroupBy0Min1(), 1, 7)
						.addInput(0,1,true,6)
						.out(0,1,true, new Tuple2by0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		mbIt.closeWith(minMsgs.map(new LogicalInputIdFiller<>(1))
			.union(mbIt0.filter(new FilterFunction<ElementOrEvent<Tuple2<Integer, Integer>>>() {
					@Override
					public boolean filter(ElementOrEvent<Tuple2<Integer, Integer>> tuple2ElementOrEvent) throws Exception {
						return false;
					}
				}).returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
			));

		//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_2 = labels_1
//				.map(new LogicalInputIdFiller<>(0))
//				.union(minMsgs.map(new LogicalInputIdFiller<>(1)))
//				.setConnectionType(new FlinkPartitioner<>())
//				//.setConnectionType(new Tuple2by0())
//				.bt("updates_2", Util.tpe(), new BagOperatorHost<>(new Join(){
//					@Override
//					protected void udf(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
//						if (a.f1 > b.f1) {
//							out.collectElement(b);
//						}
//					}
//				}, 1, 8)
//								.addInput(0,1,true,4)
//								.addInput(1,1,true,7)
//								.out(0,1,true, new Tuple2by0(para)) // To labels_2 update
//								.out(1,1,true, new Forward<>(1)) // to nonEmpty
//						        .out(2,1,false, new Forward<>(para)) // back-edge
//				)
//				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
//				.setConnectionType(new FlinkPartitioner<>())
//				.split(new CondOutputSelector<>());

		mbIt.closeWith(mbSplit.select("0").map(new LogicalInputIdFiller<>(2)));

		//     labels_2 = labels_1.update(updates_2)
//		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_2 = labels_1
//				.map(new LogicalInputIdFiller<>(0))
//				.union(updates_2.select("0").map(new LogicalInputIdFiller<>(1)))
//				.setConnectionType(new FlinkPartitioner<>())
//				//.setConnectionType(new Tuple2by0())
//				.bt("labels_2",Util.tpe(),new BagOperatorHost<>(new UpdateJoin(),1,9)
//								.addInput(0,1,true,4) // labels_1
//								.addInput(1,1,true,8) // updates_2
//								.out(0,1,false, new Forward<>(para)) // back-edge
//								.out(1,2,false, new Forward<>(1)) // out of the loop
//				)
//				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
//				.setConnectionType(new FlinkPartitioner<>())
//				.split(new CondOutputSelector<>());

		DataStream<ElementOrEvent<Boolean>> nonEmpty = mbSplit.select("1")
				//.setConnectionType(new gg.partitioners.Random<>())  //ez azert nem kell, mert itt huzzuk ossze 1-re a para-t
				.bt("nonEmpty",Util.tpe(),
						new BagOperatorHost<>(
								new NonEmpty<Tuple2<Integer, Integer>>(), 1, 10)
								.addInput(0, 1, true, 15)
								.out(0,1,true, new Forward<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
				.setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = nonEmpty
				//.setConnectionType(new gg.partitioners.Random<>())  // ez azert nem kell, mert az input es mi is 1-es para-val vagyunk
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 11)
								.addInput(0, 1, true, 10))
				.setParallelism(1);

		updatesIt.closeWith(mbSplit.select("2").map(new LogicalInputIdFiller<>(1))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
			.union(updates_1.filter(new FilterFunction<ElementOrEvent<Tuple2<Integer, Integer>>>() {
					@Override
					public boolean filter(ElementOrEvent<Tuple2<Integer, Integer>> tuple2ElementOrEvent) throws Exception {
						return false;
					}
				}).returns(TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Integer>>>(){}))
			));

		// --- Iteration ends here ---


		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> result = mbSplit.select("3");

		result
				//.setConnectionType(new gg.partitioners.Random<>())
				.bt("print", Util.tpe(), new BagOperatorHost<Tuple2<Integer, Integer>, Unit>(new Print<>("result"), 2, 12)
						.addInput(0,1,true,15)).setParallelism(1);

		result
				.bt("AssertBagEquals", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<>(
						Tuple2.of(0,0),
						Tuple2.of(1,0),
						Tuple2.of(2,0),
						Tuple2.of(3,0),
						Tuple2.of(4,0),
						Tuple2.of(5,5),
						Tuple2.of(6,5),
						Tuple2.of(7,5)
				), 2, 13)
						.addInput(0,1,true,15)).setParallelism(1);

		CFLConfig.getInstance().setNumToSubscribe();

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
