package gg.jobs;

import gg.*;
import gg.operators.*;
import gg.partitioners.Tuple2by0;
import gg.util.LogicalInputIdFiller;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
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


// SSA
// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels_0 = vertices.map(x => (x,x))
// updates_0 = labels
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


public class ConnectedComponents {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectedComponents.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1); ///////////////////////// todo: kicommentezni

		final int bufferTimeout = 0;

		env.setBufferTimeout(bufferTimeout);

		CFLConfig.getInstance().terminalBBId = 2;
		env.addSource(new KickoffSource(0,1)).addSink(new DiscardingSink<>());

		@SuppressWarnings("unchecked")
//		Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{Tuple2.of(0,1)};
		Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{
				Tuple2.of(0,1),
				Tuple2.of(1,2)
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
								Util.tpe(), new Bagify<>());

		DataStream<ElementOrEvent<Integer>> vertices0 = edges
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("edges.map((x,y) => x)", Util.tpe(), new BagOperatorHost<>(new FlatMap<Tuple2<Integer, Integer>, Integer>(){
			@Override
			public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(e.f0);
			}
		}, 0)
			.addInput(0,0,true)
			.out(0,0,true));

		DataStream<ElementOrEvent<Integer>> vertices = vertices0
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("vertices0.distinct", Util.tpe(),
				new BagOperatorHost<Integer, Integer>(new Distinct<>(), 0)
						.addInput(0,0,true)
						.out(0,0,true));

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_0 = vertices
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("x => (x,x)", Util.tpe(), new BagOperatorHost<>(new FlatMap<Integer,Tuple2<Integer,Integer>>(){
			@Override
			public void pushInElement(Integer e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(Tuple2.of(e,e));
			}
		}, 0)
			.addInput(0,0,true)
			.out(0,0,true));

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_0 = labels_0
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("Id", Util.tpe(), new BagOperatorHost<>(new IdMap<Tuple2<Integer, Integer>>(), 0)
				.addInput(0,0,true)
				.out(0,0,true));


		// --- Iteration starts here ---

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> labelsIt = labels_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_1 = labelsIt
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("labels_1", Util.tpe(), new PhiNode2<Tuple2<Integer, Integer>>(1)
				.addInput(0,0,false)
				.addInput(1,1,false)
				.out(0,1,true) // (this goes to two places, but none of them is conditional)
		);

		IterativeStream<ElementOrEvent<Tuple2<Integer, Integer>>> updatesIt = updates_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_1 = updatesIt
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("updates_1", Util.tpe(), new PhiNode2<Tuple2<Integer, Integer>>(1)
				.addInput(0,0,false)
				.addInput(1,1,false)
				.out(0,1,true)
		);

		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> msgs = edges
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_1.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new Tuple2by0())
				.bt("msgs", Util.tpe(), new BagOperatorHost<>(new Join(){
					@Override
					protected void udf(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
						out.collectElement(Tuple2.of(b.f1, a.f1));
					}
				}, 1)
				.addInput(0,0,false) // edges
				.addInput(1,1,true)
				.out(0,1,true)
				); // updates_1

		//todo: combiner
		DataStream<ElementOrEvent<Tuple2<Integer, Integer>>> minMsgs = msgs
				.setConnectionType(new Tuple2by0())
				.bt("minMsgs", Util.tpe(), new BagOperatorHost<>(new GroupBy0Min1(), 1)
						.addInput(0,1,true)
						.out(0,1,true));

		//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> updates_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(minMsgs.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new Tuple2by0())
				.bt("updates_2", Util.tpe(), new BagOperatorHost<>(new Join(){
					@Override
					protected void udf(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
						if (a.f1 > b.f1) {
							out.collectElement(b);
						}
					}
				}, 1)
								.addInput(0,1,true)
								.addInput(1,1,true)
								.out(0,1,true) // To labels_2 update and to nonEmpty
						        .out(1,1,false) // back-edge
				).split(new CondOutputSelector<>());

		//     labels_2 = labels_1.update(updates_2)
		SplitStream<ElementOrEvent<Tuple2<Integer, Integer>>> labels_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_2.select("0").map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new Tuple2by0())
				.bt("labels_2",Util.tpe(),new BagOperatorHost<>(new UpdateJoin(),1)
								.addInput(0,1,true) // labels_1
								.addInput(1,1,true) // updates_2
								.out(0,1,false) // back-edge
								.out(1,2,false) // out of the loop
				).split(new CondOutputSelector<>());

		labelsIt.closeWith(labels_2.select("0").map(new LogicalInputIdFiller<>(1)));
		updatesIt.closeWith(updates_2.select("1").map(new LogicalInputIdFiller<>(1)));

		// exit cond

		DataStream<ElementOrEvent<Boolean>> nonEmpty = updates_2.select("0")
				//.setConnectionType(new gg.partitioners.Random<>())  //ez azert nem kell, mert itt huzzuk ossze 1-re a para-t
				.bt("nonEmpty",Util.tpe(),
						new BagOperatorHost<>(
								new NonEmpty<Tuple2<Integer, Integer>>(), 1)
								.addInput(0, 1, true)
								.out(0,1,true)).setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = nonEmpty
				//.setConnectionType(new gg.partitioners.Random<>())  // ez azert nem kell, mert az input es mi is 1-es para-val vagyunk
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1)
								.addInput(0, 1, true)).setParallelism(1);

		// --- Iteration ends here ---

		labels_2.select("1")
				.setConnectionType(new gg.partitioners.Random<>())
				.bt("print labels_2", Util.tpe(), new BagOperatorHost<Tuple2<Integer, Integer>, Unit>(new Print<>("labels_2"), 2)
				.addInput(0,1,false));

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
