package eu.stratosphere.labyrinth.jobs;

import eu.stratosphere.labyrinth.ElementOrEvent;
import eu.stratosphere.labyrinth.LabyNode;
import eu.stratosphere.labyrinth.LabySource;
import eu.stratosphere.labyrinth.operators.CFAwareFileSinkGen;
import eu.stratosphere.labyrinth.operators.DistinctInt;
import eu.stratosphere.labyrinth.operators.GroupBy0Sum1TupleIntInt;
import eu.stratosphere.labyrinth.operators.IdMap;
import eu.stratosphere.labyrinth.operators.IncMap;
import eu.stratosphere.labyrinth.operators.Join;
import eu.stratosphere.labyrinth.operators.LargerThan;
import eu.stratosphere.labyrinth.operators.OpWithSingletonSide;
import eu.stratosphere.labyrinth.operators.SumDouble;
import eu.stratosphere.labyrinth.operators.Union;
import eu.stratosphere.labyrinth.operators.UpdateJoinTupleIntDouble;
import eu.stratosphere.labyrinth.partitioners.Always0;
import eu.stratosphere.labyrinth.partitioners.Broadcast;
import eu.stratosphere.labyrinth.partitioners.RoundRobin;
import eu.stratosphere.labyrinth.partitioners.TupleIntIntBy0;
import eu.stratosphere.labyrinth.util.TupleIntDouble;
import eu.stratosphere.labyrinth.util.TupleIntIntInt;
import eu.stratosphere.labyrinth.CFLConfig;
import eu.stratosphere.labyrinth.KickoffSource;
import eu.stratosphere.labyrinth.operators.ClickLogReader2;
import eu.stratosphere.labyrinth.operators.ConditionNode;
import eu.stratosphere.labyrinth.operators.CountCombiner;
import eu.stratosphere.labyrinth.operators.FlatMap;
import eu.stratosphere.labyrinth.operators.GroupBy0ReduceTupleIntDouble;
import eu.stratosphere.labyrinth.operators.JoinTupleIntDouble;
import eu.stratosphere.labyrinth.operators.JoinTupleIntInt;
import eu.stratosphere.labyrinth.operators.OuterJoinTupleIntDouble;
import eu.stratosphere.labyrinth.operators.SingletonBagOperator;
import eu.stratosphere.labyrinth.operators.SmallerThan;
import eu.stratosphere.labyrinth.operators.Sum;
import eu.stratosphere.labyrinth.operators.SumCombinerDouble;
import eu.stratosphere.labyrinth.partitioners.Forward;
import eu.stratosphere.labyrinth.partitioners.IntegerBy0;
import eu.stratosphere.labyrinth.partitioners.Tuple2by0;
import eu.stratosphere.labyrinth.partitioners.TupleIntDoubleBy0;
import eu.stratosphere.labyrinth.util.TupleIntInt;
import eu.stratosphere.labyrinth.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
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
 *       .map((id, newRank) => (id, d * newrank + (1-d) * initWeight)) // damping
 *       .rightOuterJoin(PR).map((id, newRank, oldRank) =>
 *          if (newRank == null)
 *            (id, oldRank)
 *          else
 *            (id, newRank)
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
 *     msgsDampened = msgsReduced.map((id, newRank) => (id, d * newrank + (1-d) * initWeight))
 *     msgsJoined = msgsDampened rightOuterJoin PR_2
 *     newPR = msgsJoined.map((id, newRank, oldRank) =>
 *       if (newRank == null)
 *         (id, oldRank)
 *       else
 *         (id, newRank)
 *     newOldJoin = newPR join PR
 *     changes = newOldJoin.map((id, newRank, oldRank) => abs(newRank - oldRank))
 *     totalChange = changes.sum()
 *     PR_3 = newPR
 *     innerExitCond = totalChange > epsilon
 *   While innerExitCond
 *   // BB 3
 *   notFirstDayBool = day_2 != 1
 *   If (notFirstDayBool)
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
    private static TypeInformation<ElementOrEvent<Boolean>> typeInfoBoolean = TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){});
    private static TypeInformation<ElementOrEvent<Unit>> typeInfoUnit = TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){});
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
        PojoTypeInfo.registerCustomSerializer(TupleIntIntInt.class, TupleIntIntInt.TupleIntIntIntSerializer.class);
        PojoTypeInfo.registerCustomSerializer(TupleIntDouble.class, TupleIntDouble.TupleIntDoubleSerializer.class);


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

        double epsilon = 0.0000000001;

        // -- Outer iteration starts here --   BB 1

        LabyNode<TupleIntDouble, TupleIntDouble> yesterdayPR_2 =
                LabyNode.phi("yesterdayPR_2", 1, new Forward<>(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(yesterdayPR_1, false);

        LabyNode<Integer, Integer> day_2 =
                LabyNode.phi("day_2", 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(day_1, false)
                .setParallelism(1);

        LabyNode<Integer, TupleIntInt> edges_read =
                new LabyNode<>("edges_read", new ClickLogReader2(pref + "/input/"), 1, new RoundRobin<>(para), integerSer, typeInfoTupleIntInt)
                        .addInput(day_2, true, false);

        LabyNode<TupleIntInt, TupleIntInt> edges =
                new LabyNode<>("edges", new IdMap<>(), 1, new RoundRobin<>(para), tupleIntIntSer, typeInfoTupleIntInt)
                .addInput(edges_read, true, false);

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

        TypeInformation<ElementOrEvent<Tuple2<Integer, Either<Double, TupleIntInt>>>> joinPrepTypeInfo =
                TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<Integer, Either<Double, TupleIntInt>>>>(){});

        LabyNode<TupleIntIntInt, Tuple2<Integer, Either<Double, TupleIntInt>>> edgesWithDeg_prep =
            new LabyNode<>("edgesWithDeg_prep", new FlatMap<TupleIntIntInt, Tuple2<Integer, Either<Double, TupleIntInt>>>() {
                @Override
                public void pushInElement(TupleIntIntInt e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(Tuple2.of(e.f0, Either.Right(TupleIntInt.of(e.f1, e.f2))));
                }
            }, 1, new Forward<>(para), tupleIntIntIntSer, joinPrepTypeInfo)
                .addInput(edgesWithDeg, true, false);

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
                new LabyNode<>("edgesFromToUnioned", new Union<>(), 1, new Forward<>(para), integerSer, typeInfoInt)
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

        LabyNode<TupleIntDouble, TupleIntDouble> PR_2 =
                LabyNode.phi("PR_2", 2, new Forward<>(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(PR_1, false, false);

        TypeSerializer<Tuple2<Integer, Either<Double, TupleIntInt>>> joinPrepSerializer =
                TypeInformation.of(new TypeHint<Tuple2<Integer, Either<Double, TupleIntInt>>>(){}).createSerializer(new ExecutionConfig());

        LabyNode<TupleIntDouble, Tuple2<Integer, Either<Double, TupleIntInt>>> PR_2_prep =
            new LabyNode<>("PR_2_prep", new FlatMap<TupleIntDouble, Tuple2<Integer, Either<Double, TupleIntInt>>>() {
                @Override
                public void pushInElement(TupleIntDouble e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(Tuple2.of(e.f0, Either.Left(e.f1)));
                }
            }, 2, new Forward<>(para), tupleIntDoubleSer, joinPrepTypeInfo)
                .addInput(PR_2, true, false);

        // PR_2_Joined = PR_2 join edgesWithDeg
        // msgs = PR_2_Joined.map((from, to, degree, rank) => (to, rank/degree))
        LabyNode<Tuple2<Integer, Either<Double, TupleIntInt>>, TupleIntDouble> msgs =
            new LabyNode<>("msgs", new Join<Either<Double, TupleIntInt>, TupleIntDouble>() {
                @Override
                protected void udf(Tuple2<Integer, Either<Double, TupleIntInt>> b, Tuple2<Integer, Either<Double, TupleIntInt>> a) {
                    // vigyazat, meg van cserelve az a es a b
                    assert a.f1.isLeft();
                    assert b.f1.isRight();
                    int to = b.f1.right().f0;
                    double rank = a.f1.left();
                    int degree = b.f1.right().f1;
                    out.collectElement(TupleIntDouble.of(to, rank/degree));
                }
            }, 2, new Tuple2by0<>(para), joinPrepSerializer, typeInfoTupleIntDouble)
                .addInput(edgesWithDeg_prep, false, false)
                .addInput(PR_2_prep, true, false);

        LabyNode<TupleIntDouble, TupleIntDouble> msgsReduced =
            new LabyNode<>("msgsReduced", new GroupBy0ReduceTupleIntDouble() {
                @Override
                protected void reduceFunc(TupleIntDouble e, double g) {
                    hm.replace(e.f0, e.f1 + g);
                }
            }, 2, new TupleIntDoubleBy0(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(msgs, true, false);

        LabyNode<TupleIntDouble, TupleIntDouble> msgsDampened =
            new LabyNode<>("msgsDampened", new OpWithSingletonSide<TupleIntDouble, TupleIntDouble>(tupleIntDoubleSer) {
                @Override
                protected void pushInElementWithSingletonSide(TupleIntDouble e, TupleIntDouble side) {
                    assert side.f0 == -1;
                    double initWeight = side.f1;
                    double newRank = e.f1;
                    out.collectElement(TupleIntDouble.of(e.f0, d * newRank + (1-d) * initWeight));
                }
            }, 2, null, tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(initWeightTupleIntDouble, false, false, new Broadcast<>(para))
                .addInput(msgsReduced, true, false, new Forward<>(para));

        LabyNode<TupleIntDouble, TupleIntDouble> newPR =
            new LabyNode<>("newPR", new UpdateJoinTupleIntDouble(), 2, new TupleIntDoubleBy0(para), tupleIntDoubleSer, typeInfoTupleIntDouble)
                .addInput(PR_2, true, false)
                .addInput(msgsDampened, true, false);

        LabyNode<TupleIntDouble, Double> changes =
                new LabyNode<>("changes", new JoinTupleIntDouble<Double>() {
                    @Override
                    protected void udf(double b, TupleIntDouble p) {
                        out.collectElement(Math.abs(b - p.f1));
                    }
                }, 2, new TupleIntDoubleBy0(para), tupleIntDoubleSer, typeInfoDouble)
                .addInput(newPR, true, false)
                .addInput(PR_2, true, false);

        LabyNode<Double, Double> totalChangeCombiner =
                new LabyNode<>("totalChangeCombiner", new SumCombinerDouble(), 2, new Forward<>(para), doubleSer, typeInfoDouble)
                .addInput(changes, true, false);

        LabyNode<Double, Double> totalChange =
                new LabyNode<>("totalChange", new SumDouble(), 2, new Always0<>(1), doubleSer, typeInfoDouble)
                .addInput(totalChangeCombiner, true, false)
                .setParallelism(1);

        // PR_3 is "optimized out"

        PR_2.addInput(newPR, false, true, Collections.singleton(1));

        LabyNode<Double, Boolean> innerExitCondBool =
            new LabyNode<>("innerExitCondBool", new LargerThan(epsilon), 2, new Always0<>(1), doubleSer, typeInfoBoolean)
                .addInput(totalChange, true, false)
                .setParallelism(1);

        LabyNode<Boolean, Unit> innerExitCond =
            new LabyNode<>("innerExitCond", new ConditionNode(2, 3), 2, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
            .addInput(innerExitCondBool, true, false)
            .setParallelism(1);

        // -- Inner iteration ends here --   BB 3


        // Debug print all PRs:

//        TypeInformation<Either<Integer, TupleIntDouble>> typeInfoEitherIntTupleIntDouble = TypeInformation.of(new TypeHint<Either<Integer, TupleIntDouble>>(){});
//        TypeInformation<ElementOrEvent<Either<Integer, TupleIntDouble>>> typeInfoEoEEitherIntTupleIntDouble = TypeInformation.of(new TypeHint<ElementOrEvent<Either<Integer, TupleIntDouble>>>(){});
//        TypeSerializer<Either<Integer, TupleIntDouble>> eitherIntTupleIntDoubleSer = typeInfoEitherIntTupleIntDouble.createSerializer(new ExecutionConfig());
//        TypeInformation<ElementOrEvent<Either<Integer, Double>>> typeInfoEoEEitherIntDouble = TypeInformation.of(new TypeHint<ElementOrEvent<Either<Integer, Double>>>(){});
//
//        LabyNode<Integer, Either<Integer, TupleIntDouble>> day_2_prepXXX =
//                new LabyNode<>("day_2_prep", new FlatMap<Integer, Either<Integer, TupleIntDouble>>() {
//                    @Override
//                    public void pushInElement(Integer e, int logicalInputId) {
//                        super.pushInElement(e, logicalInputId);
//                        out.collectElement(Either.Left(e));
//                    }
//                }, 3, new Forward<>(para), integerSer, typeInfoEoEEitherIntTupleIntDouble)
//                        .addInput(day_2, false, false);
//
//        LabyNode<TupleIntDouble, Either<Integer, TupleIntDouble>> PR_2_prepXXX =
//                new LabyNode<>("PR_2_prep", new FlatMap<TupleIntDouble, Either<Integer, TupleIntDouble>>() {
//                    @Override
//                    public void pushInElement(TupleIntDouble e, int logicalInputId) {
//                        super.pushInElement(e, logicalInputId);
//                        out.collectElement(Either.Right(e));
//                    }
//                }, 3, new Forward<>(para), tupleIntDoubleSer, typeInfoEoEEitherIntTupleIntDouble)
//                .addInput(PR_2, false, true);
//
//        LabyNode<Either<Integer, TupleIntDouble>, Unit> printPR =
//                new LabyNode<Either<Integer, TupleIntDouble>, Unit>("printPR", new CFAwareFileSinkGen<TupleIntDouble>(pref + "allPRs/", tupleIntDoubleSer),
//                        3, new Always0<>(1), eitherIntTupleIntDoubleSer, typeInfoUnit)
//                .addInput(day_2_prepXXX, true, false)
//                .addInput(PR_2_prepXXX, true, false)
//                .setParallelism(1);



        LabyNode<Integer, Boolean> notFirstDayBool =
            new LabyNode<>("notFirstDayBool", new SingletonBagOperator<Integer, Boolean>() {
                @Override
                public void pushInElement(Integer e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(!e.equals(1));
                }
            }, 3, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
            .addInput(day_2, false, false) //todo: remelem jo itt igy a ket boolean
            .setParallelism(1);

        LabyNode<Boolean, Unit> notFirstDay =
            new LabyNode<>("notFirstDay", new ConditionNode(new int[]{4,5}, new int[]{5}), 3, new Always0<>(1), booleanSer, typeInfoUnit)
            .addInput(notFirstDayBool, true, false)
            .setParallelism(1);

        // -- then branch   BB 4

        // The join of joinedYesterday is merged into this operator
        LabyNode<TupleIntDouble, Double> diffs =
            new LabyNode<>("diffs", new OuterJoinTupleIntDouble<Double>() {
                @Override
                protected void inner(double b, TupleIntDouble p) {
                    out.collectElement(Math.abs(b - p.f1));
                }

                @Override
                protected void right(TupleIntDouble p) {
                    out.collectElement(p.f1);
                }

                @Override
                protected void left(double b) {
                    out.collectElement(b);
                }
            }, 4, new TupleIntDoubleBy0(para), tupleIntDoubleSer, typeInfoDouble)
            .addInput(newPR, false, true)
            .addInput(yesterdayPR_2, false, true);

        LabyNode<Double, Double> sumCombiner =
            new LabyNode<>("sumCombiner", new SumCombinerDouble(), 4, new Forward<>(para), doubleSer, typeInfoDouble)
                .addInput(diffs, true, false);

        LabyNode<Double, Double> sum =
            new LabyNode<>("sum", new SumDouble(), 4, new Always0<>(1), doubleSer, typeInfoDouble)
                .addInput(sumCombiner, true, false)
                .setParallelism(1);

        TypeInformation<Either<Integer, Double>> typeInfoEitherIntDouble = TypeInformation.of(new TypeHint<Either<Integer, Double>>(){});
        TypeInformation<ElementOrEvent<Either<Integer, Double>>> typeInfoEoEEitherIntDouble = TypeInformation.of(new TypeHint<ElementOrEvent<Either<Integer, Double>>>(){});
        TypeSerializer<Either<Integer, Double>> eitherIntDoubleSer = typeInfoEitherIntDouble.createSerializer(new ExecutionConfig());

        LabyNode<Integer, Either<Integer, Double>> day_2_prep =
                new LabyNode<>("day_2_prep", new FlatMap<Integer, Either<Integer, Double>>() {
                    @Override
                    public void pushInElement(Integer e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(Either.Left(e));
                    }
                }, 4, new Forward<>(para), integerSer, typeInfoEoEEitherIntDouble)
                .addInput(day_2, false, true);

        LabyNode<Double, Either<Integer, Double>> sum_prep =
            new LabyNode<>("day_2_prep", new FlatMap<Double, Either<Integer, Double>>() {
                @Override
                public void pushInElement(Double e, int logicalInputId) {
                    super.pushInElement(e, logicalInputId);
                    out.collectElement(Either.Right(e));
                }
            }, 4, new Forward<>(para), doubleSer, typeInfoEoEEitherIntDouble)
                .addInput(sum, true, false);

        LabyNode<Either<Integer, Double>, Unit> printSum =
                new LabyNode<>("printSum", new CFAwareFileSinkGen<>(pref + "out/diff_", doubleSer), 4, new Always0<>(1), eitherIntDoubleSer, typeInfoUnit)
                .addInput(day_2_prep, true, false)
                .addInput(sum_prep, true, false)
                .setParallelism(1);

        // -- end of then branch   BB 5

        // (We "optimize away" yesterdayCounts_3, since it would be an IdMap)
        yesterdayPR_2.addInput(PR_2, false, true);

        LabyNode<Integer, Integer> day_3 =
                new LabyNode<>("day_3", new IncMap(), 5, new Always0<>(1), integerSer, typeInfoInt)
                .addInput(day_2, false, false)
                .setParallelism(1);

        day_2.addInput(day_3, false, true);

        LabyNode<Integer, Boolean> outerExitCondBool =
                new LabyNode<>("outerExitCondBool", new SmallerThan(Integer.parseInt(args[1]) + 1), 5, new Always0<>(1), integerSer, typeInfoBoolean)
                .addInput(day_3, true, false)
                .setParallelism(1);

        LabyNode<Boolean, Unit> outerExitCond =
                new LabyNode<>("outerExitCond", new ConditionNode(new int[]{1,2}, new int[]{6}), 5, new Always0<>(1), booleanSer, typeInfoUnit)
                .addInput(outerExitCondBool, true, false)
                .setParallelism(1);

        // -- Iteration ends here   BB 6

        // Itt nincs semmi operator. (A kiirast a BB 4-ben csinaljuk.)

        LabyNode.printOperatorIDNameMapping();

        LabyNode.translateAll();

        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
