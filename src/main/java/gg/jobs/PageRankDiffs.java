package gg.jobs;

import gg.CFLConfig;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.LabyNode;
import gg.LabySource;
import gg.operators.CFAwareFileSink;
import gg.operators.ClickLogReader;
import gg.operators.ClickLogReader2;
import gg.operators.ConditionNode;
import gg.operators.CountCombiner;
import gg.operators.DistinctInt;
import gg.operators.FlatMap;
import gg.operators.GroupBy0ReduceTupleIntDouble;
import gg.operators.GroupBy0Sum1TupleIntInt;
import gg.operators.IdMap;
import gg.operators.IncMap;
import gg.operators.Join;
import gg.operators.JoinTupleIntInt;
import gg.operators.OpWithSideInput;
import gg.operators.OpWithSingletonSide;
import gg.operators.OuterJoinTupleIntInt;
import gg.operators.SingletonBagOperator;
import gg.operators.SmallerThan;
import gg.operators.Sum;
import gg.operators.SumCombiner;
import gg.partitioners.Always0;
import gg.partitioners.Broadcast;
import gg.partitioners.Forward;
import gg.partitioners.IntegerBy0;
import gg.partitioners.RoundRobin;
import gg.partitioners.Tuple2by0;
import gg.partitioners.TupleIntDoubleBy0;
import gg.partitioners.TupleIntIntBy0;
import gg.util.SerializedBuffer;
import gg.util.TupleIntDouble;
import gg.util.TupleIntInt;
import gg.util.TupleIntIntInt;
import gg.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.types.Either;

import java.util.Collections;

/**
 * This is similar to ClickCountDiffs, in that we compute differences between datasets computed in consecutive iterations.
 * Here, we compare PageRanks of the graph of actual transitions between pages.
 *
 * yesterdayPR = null // the previous day's PageRank
 * d = 0.85 // damping factor
 * epsilon = 0.001
 * For day = 1 .. 365
 *   edges = readFile("click_log_" + day) // (from,to) pairs
 *   // Compute out-degrees and attach to edges
 *   edgesWithDeg = edges
 *     .map((from, to) => (from, 1))
 *     .reduceByKey(_ + _)
 *     .join(edges) // results in (from, to, degree) triples
 *   // Get all pages and init PageRank computation
 *   pages = (edges.map(_.from) union edges.map(_.to)).distinct
 *   numPages = pages.count
 *   initWeight = 1.0 / numPages
 *   PR = pages.map(id => (id, initWeight))
 *   Do
 *     newPR =
 *       PR.join(edgesWithDeg).map((from, to, degree, rank) => (to, rank/degree)) // send msgs to neighbors
 *       .reduceByKey(_ + _) // group msgs by their targets and sum them
 *       .rightOuterJoin(PR).map((id, newrank, oldrank) =>
 *          if (newrank == null)
 *            (id, oldrank)
 *          else
 *            (id, d * newrank + (1-d) * initWeight)) // apply the received msgs with damping
 *     totalChange = (newPR join PR).map((id, newRank, oldRank) => abs(newRank - oldRank)).sum() // Compute differences and sum them
 *     PR = newPR
 *   While (totalChange > epsilon)
 *   If (day != 1)
 *     diffs = (PR join yesterdayPR).map((id,today,yesterday) => abs(today - yesterday))
 *     printLine(diffs.sum)
 *   End if
 *   yesterdayPR = PR
 * End for
 *
 * SSA:
 *
 * // BB 0
 * yesterdayPR_1 = null // the previous day's PageRank
 * d = 0.85 // damping factor
 * epsilon = 0.001
 * day_1 = 1
 * Do
 *   // BB 1
 *   day_2 = phi(day_1, day_3)
 *   yesterdayPR_2 = phi(yesterdayPR_1, yesterdayPR_3)
 *   edges = readFile("click_log_" + day) // (from,to) pairs
 *   edgesMapped = edges.map((from, to) => (from, 1))
 *   edgesMappedReduced = edgesMapped.reduceByKey(_ + _)
 *   edgesWithDeg = edgesMappedReduced join edges // (from, to, degree) triples
 *   // Get all pages and init PageRank computation
 *   edgesFromMapped = edges.map(_.from)
 *   edgesToMapped = edges.map(_.to)
 *   edgesFromToUnioned = edgesFromMapped union edgesToMapped
 *   pages = pagesFromToUnioned.distinct
 *   numPages = pages.count
 *   initWeight = 1.0 / numPages
 *   PR_1 = pages.map(id => (id, initWeight))
 *   Do
 *     // BB 2
 *     PR_2 = phi(PR_1, PR_3)
 *     PR_2_Joined = PR_2 join edgesWithDeg
 *     msgs = PR_2_Joined.map((from, to, degree, rank) => (to, rank/degree))
 *     msgsReduced = msgs.reduceByKey(_ + _)
 *     msgsJoined = msgsReduced.rightOuterJoin(PR)
 *     newPR = msgsJoined.map((id, newrank, oldrank) =>
 *       if (newrank == null)
 *         (id, oldrank)
 *       else
 *         (id, d * newrank + (1-d) * initWeight))
 *     newOldJoin = newPR join PR
 *     changes = newOldJoin.map((id, newRank, oldRank) => abs(newRank - oldRank))
 *     totalChange = changes.sum()
 *     PR_3 = newPR
 *     innerExitCond = totalChange > epsilon
 *   While innerExitCond
 *   // BB 3
 *   ifCond = day_2 != 1
 *   If (ifCond)
 *     // BB 4
 *     joinedYesterday = PR_3 join yesterdayPR_2
 *     diffs = joinedYesterday.map((id,today,yesterday) => abs(today - yesterday))
 *     summed = diffs.sum
 *     printLine(summed)
 *   End if
 *   // BB 5
 *   yesterdayPR_3 = PR_2
 *   day_3 = day_2 + 1
 *   outerExitCond = day_3 < 365
 * While outerExitCond
 * // BB 6
 */

public class PageRankDiffs {

    private static TypeInformation<ElementOrEvent<TupleIntInt>> typeInfoTupleIntInt = TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){});
    private static TypeInformation<ElementOrEvent<TupleIntIntInt>> typeInfoTupleIntIntInt = TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntIntInt>>(){});
    private static TypeInformation<ElementOrEvent<Integer>> typeInfoInt = TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){});
    private static TypeInformation<ElementOrEvent<Double>> typeInfoDouble = TypeInformation.of(new TypeHint<ElementOrEvent<Double>>(){});
    private static TypeInformation<ElementOrEvent<TupleIntDouble>> typeInfoTupleIntDouble = TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntDouble>>(){});

    private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<Double> doubleSer = TypeInformation.of(Double.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<TupleIntInt> tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer();
    private static TypeSerializer<TupleIntIntInt> tupleIntIntIntSer = new TupleIntIntInt.TupleIntIntIntSerializer();
    private static TypeSerializer<TupleIntDouble> tupleIntDoubleSer = new TupleIntDouble.TupleIntDoubleSerializer();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        final String pref = args[0] + "/";


        PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
        PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);


        CFLConfig.getInstance().terminalBBId = 6;
        KickoffSource kickoffSrc = new KickoffSource(0, 1, 2);
        env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

        final int para = env.getParallelism();


        // BB 0

        @SuppressWarnings("unchecked")
        LabySource<TupleIntDouble> yesterdayPR_1 =
                new LabySource<>(env.fromCollection(Collections.emptyList(), TypeInformation.of(TupleIntDouble.class)), 0, typeInfoTupleIntDouble);

        LabySource<Integer> day_1 =
                new LabySource<>(env.fromCollection(Collections.singletonList(1)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                        .setParallelism(1);

        double d = 0.85;

        double epsilon = 0.001;

        // -- Outer iteration starts here --   BB 1

        LabyNode<TupleIntDouble, TupleIntDouble> yesterdayPR_2 =
                LabyNode.phi("yesterdayPR_2", 1, new Forward<>(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(yesterdayPR_1, false);

        LabyNode<Integer, Integer> day_2 =
                LabyNode.phi("day_2", 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(day_1, false)
                .setParallelism(1);
        //todo: add back edge

        LabyNode<Integer, TupleIntInt> edges =
                new LabyNode<>("edges", new ClickLogReader2(pref + "in/clickLog_"), 1, new RoundRobin<>(para), integerSer, typeInfoTupleIntInt)
                        .addInput(day_2, true, false);
                        //.setParallelism(1);

        LabyNode<TupleIntInt, TupleIntInt> edgesMapped =
                new LabyNode<>("edgesMapped", new FlatMap<TupleIntInt, TupleIntInt>() {
                    @Override
                    public void pushInElement(TupleIntInt e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(TupleIntInt.of(e.f0, 1));
                    }
                }, 1, new Forward<>(para), tupleIntIntSer, typeInfoTupleIntInt)
                .addInput(edges, true, false);

        LabyNode<TupleIntInt, TupleIntInt> edgesMappedReduced =
                new LabyNode<>("edgesMappedReduced", new GroupBy0Sum1TupleIntInt(), 1, new TupleIntIntBy0(para), tupleIntIntSer, typeInfoTupleIntInt)
                .addInput(edgesMapped, true, false);

        // (from, to, degree)
        LabyNode<TupleIntInt, TupleIntIntInt> edgesWithDeg =
                new LabyNode<>("edgesWithDeg", new JoinTupleIntInt<TupleIntIntInt>() {
                    @Override
                    protected void udf(int b, TupleIntInt p) {
                        out.collectElement(TupleIntIntInt.of(p.f0, b, p.f1));
                    }
                }, 1, new TupleIntIntBy0(para), tupleIntIntSer, typeInfoTupleIntIntInt)
                .addInput(edges, true, false)
                .addInput(edgesMappedReduced, true, false);

        LabyNode<TupleIntInt, Integer> edgesFromMapped = new LabyNode<>("edgesFromMapped", new FlatMap<TupleIntInt, Integer>() {
            @Override
            public void pushInElement(TupleIntInt e, int logicalInputId) {
                super.pushInElement(e, logicalInputId);
                out.collectElement(e.f0);
            }
        }, 1, new Forward<>(para), tupleIntIntSer, typeInfoInt)
                .addInput(edges, true, false);

        LabyNode<TupleIntInt, Integer> edgesToMapped = new LabyNode<>("edgesToMapped", new FlatMap<TupleIntInt, Integer>() {
            @Override
            public void pushInElement(TupleIntInt e, int logicalInputId) {
                super.pushInElement(e, logicalInputId);
                out.collectElement(e.f1);
            }
        }, 1, new Forward<>(para), tupleIntIntSer, typeInfoInt)
                .addInput(edges, true, false);

        LabyNode<Integer, Integer> edgesFromToUnioned =
                new LabyNode<>("edgesFromToUnioned", new IdMap<>(), 1, new Forward<>(para), integerSer, typeInfoInt)
                .addInput(edgesFromMapped, true, false)
                .addInput(edgesToMapped, true, false);

        LabyNode<Integer, Integer> pages =
                new LabyNode<>("pages", new DistinctInt(), 1, new IntegerBy0(para), integerSer, typeInfoInt)
                .addInput(edgesFromToUnioned, true, false);

        LabyNode<Integer, Integer> numPagesCombiner =
                new LabyNode<>("numPagesCombiner", new CountCombiner<>(), 1, new Forward<>(para), integerSer, typeInfoInt)
                .addInput(pages, true, false);

        LabyNode<Integer, Integer> numPages =
                new LabyNode<>("numPages", new Sum(), 1, new Always0<>(1), integerSer, typeInfoInt)
                .addInput(numPagesCombiner, true, false)
                .setParallelism(1);

        LabyNode<Integer, Double> initWeight =
                new LabyNode<>("initWeight", new SingletonBagOperator<Integer, Double>() {
                    @Override
                    public void pushInElement(Integer numPages, int logicalInputId) {
                        super.pushInElement(numPages, logicalInputId);
                        out.collectElement(1.0 / numPages);
                    }
                }, 1, new Always0<>(1), integerSer, typeInfoDouble)
                .addInput(numPages, true, false)
                .setParallelism(1);

        // Ez csak a union miatt kell (hogy azonosak legyenek a tipusok)
        LabyNode<Double, TupleIntDouble> initWeightTupleIntDouble =
                new LabyNode<>("initWeightTupleIntDouble", new FlatMap<Double, TupleIntDouble>() {
                    @Override
                    public void pushInElement(Double e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(TupleIntDouble.of(-1, e));
                    }
                }, 1, new Forward<>(para), doubleSer, typeInfoTupleIntDouble)
                .addInput(initWeight, true, false);

        // Ez csak a union miatt kell (hogy azonosak legyenek a tipusok)
        LabyNode<Integer, TupleIntDouble> pagesTupleIntDouble =
                new LabyNode<>("pagesTupleIntDouble", new FlatMap<Integer, TupleIntDouble>() {
                    @Override
                    public void pushInElement(Integer e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(TupleIntDouble.of(e, -1.0));
                    }
                }, 1, new Forward<>(para), integerSer, typeInfoTupleIntDouble)
                .addInput(pages, true, false);

        LabyNode<TupleIntDouble, TupleIntDouble> PR_1 =
                new LabyNode<>("PR_1", new OpWithSingletonSide<TupleIntDouble, TupleIntDouble>(tupleIntDoubleSer) {
                    @Override
                    protected void pushInElementWithSingletonSide(TupleIntDouble e, TupleIntDouble side) {
                        assert e.f1 == -1.0;
                        assert side.f0 == -1;
                        out.collectElement(TupleIntDouble.of(e.f0, side.f1));
                    }
                }, 1, null, tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(initWeightTupleIntDouble, true, false, new Broadcast<>(para))
                .addInput(pagesTupleIntDouble, true, false, new Forward<>(para));

        // -- Inner iteration starts here --   BB 2

        /*
 *     PR_2 = phi(PR_1, PR_3)
 *     PR_2_Joined = PR_2 join edgesWithDeg
 *     msgs = PR_2_Joined.map((from, to, degree, rank) => (to, rank/degree))
 *     msgsReduced = msgs.reduceByKey(_ + _)
         */

        LabyNode<TupleIntDouble, TupleIntDouble> PR_2 =
                LabyNode.phi("PR_2", 2, new Forward<>(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(PR_1, false, false);
        //todo: add back edge

        TypeInformation<ElementOrEvent<Tuple2<Integer, Either<Double, TupleIntInt>>>> joinPrepTypeInfo =
                TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Either<Double, TupleIntInt>>>>(){});

        TypeSerializer<Tuple2<Integer, Either<Double, TupleIntInt>>> joinPrepSerializer =
                TypeInformation.of(new TypeHint<Tuple2<Integer, Either<Double, TupleIntInt>>>(){}).createSerializer(new ExecutionConfig());

        //TODO: majd leellenorizni, hogy a megfelelo serializerek jonnek elo:
        // - nincs Kryo
        // - az Either a sajat spec serializerevel van serializalva

        LabyNode<TupleIntDouble, Tuple2<Integer, Either<Double, TupleIntInt>>> PR_2_prep =
            new LabyNode<>("PR_2_prep", new FlatMap<TupleIntDouble, Tuple2<Integer, Either<Double, TupleIntInt>>>() {
                @Override
                public void pushInElement(TupleIntDouble e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(Tuple2.of(e.f0, Either.Left(e.f1)));
                }
            }, 2, new Forward<>(para), tupleIntDoubleSer, joinPrepTypeInfo)
                .addInput(PR_2, true, false);

        LabyNode<TupleIntIntInt, Tuple2<Integer, Either<Double, TupleIntInt>>> edgesWithDeg_prep =
            new LabyNode<>("edgesWithDeg_prep", new FlatMap<TupleIntIntInt, Tuple2<Integer, Either<Double, TupleIntInt>>>() {
                @Override
                public void pushInElement(TupleIntIntInt e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(Tuple2.of(e.f0, Either.Right(TupleIntInt.of(e.f1, e.f2))));
                }
            }, 2, new Forward<>(para), tupleIntIntIntSer, joinPrepTypeInfo)
            .addInput(edgesWithDeg, false, false);

        // PR_2_Joined = PR_2 join edgesWithDeg
        // msgs = PR_2_Joined.map((from, to, degree, rank) => (to, rank/degree))
        LabyNode<Tuple2<Integer, Either<Double, TupleIntInt>>, TupleIntDouble> msgs =
            new LabyNode<>("msgs", new Join<Either<Double, TupleIntInt>, TupleIntDouble>() {
                @Override
                protected void udf(Tuple2<Integer, Either<Double, TupleIntInt>> a, Tuple2<Integer, Either<Double, TupleIntInt>> b) {
                    assert a.f1.isLeft();
                    assert b.f1.isRight();
                    int to = b.f1.right().f0;
                    double rank = a.f1.left();
                    int degree = b.f1.right().f1;
                    out.collectElement(TupleIntDouble.of(to, rank/degree));
                }
            }, 2, new Tuple2by0<>(para), joinPrepSerializer, typeInfoTupleIntDouble)
                .addInput(PR_2_prep, true, false)
                .addInput(edgesWithDeg_prep, true, false);

        LabyNode<TupleIntDouble, TupleIntDouble> msgsReduced =
            new LabyNode<>("msgsReduced", new GroupBy0ReduceTupleIntDouble() {
                @Override
                protected void reduceFunc(TupleIntDouble e, double g) {
                    hm.replace(e.f0, e.f1 + g);
                }
            }, 2, new TupleIntDoubleBy0(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(msgs, true, false);




//        LabyNode<Integer, Boolean> notFirstDay =
//                new LabyNode<>("notFirstDay", new SingletonBagOperator<Integer, Boolean>() {
//                    @Override
//                    public void pushInElement(Integer e, int logicalInputId) {
//                        super.pushInElement(e, logicalInputId);
//                        out.collectElement(!e.equals(1));
//                    }
//                }, 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
//                .addInput(day_2, true, false)
//                .setParallelism(1);
//
//        LabyNode<Boolean, Unit> ifCond =
//                new LabyNode<>("ifCond", new ConditionNode(new int[]{2,3}, new int[]{3}), 1, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
//                .addInput(notFirstDay, true, false)
//                .setParallelism(1);
//
//        // -- then branch   BB 2
//
//        // The join of joinedYesterday is merged into this operator
//        LabyNode<TupleIntInt, TupleIntInt> diffs =
//                new LabyNode<>("diffs", new OuterJoinTupleIntInt() {
//                    @Override
//                    protected void inner(int b, TupleIntInt p) {
//                        out.collectElement(TupleIntInt.of(p.f0, Math.abs(b - p.f1)));
//                    }
//
//                    @Override
//                    protected void right(TupleIntInt p) {
//                        out.collectElement(p);
//                    }
//
//                    @Override
//                    protected void left(int b) {
//                        out.collectElement(TupleIntInt.of(-1, b));
//                    }
//                }, 2, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
//                .addInput(yesterdayCounts_2, false, true)
//                .addInput(counts, false, true);
//
//        LabyNode<TupleIntInt, Integer> diffsInt =
//                new LabyNode<>("diffsInt", new FlatMap<TupleIntInt, Integer>() {
//                    @Override
//                    public void pushInElement(TupleIntInt e, int logicalInputId) {
//                        super.pushInElement(e, logicalInputId);
//                        out.collectElement(e.f1);
//                    }
//                }, 2, new Forward<>(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
//                .addInput(diffs, true, false);
//
//        LabyNode<Integer, Integer> sumCombiner =
//                new LabyNode<>("sumCombiner", new SumCombiner(), 2, new Forward<>(para), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
//                .addInput(diffsInt, true, false);
//
//        LabyNode<Integer, Integer> sum =
//                new LabyNode<>("sum", new Sum(), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
//                .addInput(sumCombiner, true, false)
//                .setParallelism(1);
//
//        LabyNode<Integer, Unit> printSum =
//                new LabyNode<>("printSum", new CFAwareFileSink(pref + "out/diff_"), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
//                .addInput(day_2, false, true)
//                .addInput(sum, true, false)
//                .setParallelism(1);
//
//        // -- end of then branch   BB 3
//
//        // (We "optimize away" yesterdayCounts_3, since it would be an IdMap)
//        yesterdayCounts_2.addInput(counts, false, true);
//
//        LabyNode<Integer, Integer> day_3 =
//                new LabyNode<>("day_3", new IncMap(), 3, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
//                .addInput(day_2, false, false)
//                .setParallelism(1);
//
//        day_2.addInput(day_3, false, true);
//
//        LabyNode<Integer, Boolean> notLastDay =
//                new LabyNode<>("notLastDay", new SmallerThan(Integer.parseInt(args[1]) + 1), 3, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
//                .addInput(day_3, true, false)
//                .setParallelism(1);
//
//        LabyNode<Boolean, Unit> exitCond =
//                new LabyNode<>("exitCond", new ConditionNode(1, 4), 3, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
//                .addInput(notLastDay, true, false)
//                .setParallelism(1);
//
//        // -- Iteration ends here   BB 4
//
//        // Itt nincs semmi operator. (A kiirast a BB 4-ben csinaljuk.)
//
        LabyNode.translateAll();

//        env.execute();
    }
}
