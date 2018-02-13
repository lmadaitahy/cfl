package eu.stratosphere.labyrinth.jobs

import java.util.Collections

import eu.stratosphere.labyrinth._
import eu.stratosphere.labyrinth.operators._
import eu.stratosphere.labyrinth.partitioners._
import eu.stratosphere.labyrinth.util.{TupleIntInt, Unit}
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.TupleCsvInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.DiscardingSink

object ClickCountDiffsScala {
	private val typeInfoTupleIntInt = new TupleTypeInfo[Tuple2[Integer, Integer]](TypeInformation.of(classOf[Integer]), TypeInformation.of(classOf[Integer]))

	private val integerSer = TypeInformation.of(classOf[Integer]).createSerializer(new ExecutionConfig)
	private val booleanSer = TypeInformation.of(classOf[java.lang.Boolean]).createSerializer(new ExecutionConfig)
	private val tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer

	@throws[Exception]
	def main(args: Array[String]): scala.Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		//env.setParallelism(1);
		val pref = args(0) + "/"
		PojoTypeInfo.registerCustomSerializer(classOf[ElementOrEvent[_]], new ElementOrEvent.ElementOrEventSerializerFactory)
		PojoTypeInfo.registerCustomSerializer(classOf[TupleIntInt], classOf[TupleIntInt.TupleIntIntSerializer])
		CFLConfig.getInstance.reuseInputs = (args(2).toBoolean)
		CFLConfig.getInstance.terminalBBId = 4
		val kickoffSrc = new KickoffSource(0, 1)
		env.addSource(kickoffSrc).addSink(new DiscardingSink[Unit])
		val para = env.getParallelism
		// BB 0
		val pageAttributesStream = env.createInput[Tuple2[Integer, Integer]](new TupleCsvInputFormat[Tuple2[Integer, Integer]](new Path(pref + "in/pageAttributes.tsv"), "\n", "\t", typeInfoTupleIntInt)/*, typeInfoTupleIntInt*/).map(new MapFunction[Tuple2[Integer, Integer], TupleIntInt]() {
			@throws[Exception]
			override def map(value: Tuple2[Integer, Integer]): TupleIntInt = TupleIntInt.of(value.f0, value.f1)
		}).javaStream
		val pageAttributes = new LabySource[TupleIntInt](pageAttributesStream, 0, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {}))
		@SuppressWarnings(Array("unchecked")) val yesterdayCounts_1 = new LabySource[TupleIntInt](env.fromCollection[TupleIntInt](Seq()).javaStream, 0, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {}))
		val day_1 = new LabySource[Integer](env.fromCollection(List[Integer](1)).javaStream, 0, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).setParallelism(1)
		// -- Iteration starts here --   BB 1
		val yesterdayCounts_2 = LabyNode.phi[TupleIntInt]("yesterdayCounts_2", 1, new Forward[TupleIntInt](para), tupleIntIntSer, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {})).addInput(yesterdayCounts_1, false)
		val day_2 = LabyNode.phi[Integer]("day_2", 1, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(day_1, false).setParallelism(1)
		val visits_1 = new LabyNode[Integer, Integer]("visits_1", new ClickLogReader(pref + "in/clickLog_"), 1, new RoundRobin[Integer](para), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(day_2, true, false)
		//.setParallelism(1);
		// The inputs of the join have to be the same type (because of the union stuff), so we add a dummy tuple element.
		val visits_1_tupleized = new LabyNode[Integer, TupleIntInt]("visits_1_tupleized", new FlatMap[Integer, TupleIntInt]() {
			override def pushInElement(e: Integer, logicalInputId: Int): scala.Unit = {
				super.pushInElement(e, logicalInputId)
				out.collectElement(TupleIntInt.of(e, -1))
			}
		}, 1, new IntegerBy0(para), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {})).addInput(visits_1, true, false)
		//        LabyNode<TupleIntInt, TupleIntInt> joinedWithAttrs =
		//                new LabyNode<>("joinedWithAttrs", new JoinTupleIntInt() {
		//                    @Override
		//                    protected void udf(int b, TupleIntInt p) {
		//                        out.collectElement(TupleIntInt.of(p.f0, b));
		//                    }
		//                }, 1, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
		//                .addInput(pageAttributes, false)
		//                .addInput(visits_1_tupleized, true, false);
		//
		//        LabyNode<TupleIntInt, TupleIntInt> visits_2 =
		//                new LabyNode<>("visits_2", new FlatMap<TupleIntInt, TupleIntInt>() {
		//                    @Override
		//                    public void pushInElement(TupleIntInt e, int logicalInputId) {
		//                        super.pushInElement(e, logicalInputId);
		//                        if (e.f1 == 0) {
		//                            out.collectElement(e);
		//                        }
		//                    }
		//                }, 1, new Forward<>(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
		//                .addInput(joinedWithAttrs, true, false);
		//
		//        LabyNode<TupleIntInt, TupleIntInt> clicksMapped =
		//                new LabyNode<>("clicksMapped", new FlatMap<TupleIntInt, TupleIntInt>() {
		//                    @Override
		//                    public void pushInElement(TupleIntInt e, int logicalInputId) {
		//                        super.pushInElement(e, logicalInputId);
		//                        out.collectElement(TupleIntInt.of(e.f0, 1));
		//                    }
		//                }, 1, new Forward<>(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
		//                .addInput(visits_2, true, false);
		// The previous three operators merged into one
		val clicksMapped = new LabyNode[TupleIntInt, TupleIntInt]("joinedWithAttrs", new JoinTupleIntInt[TupleIntInt]() {
			override protected def udf(b: Int, p: TupleIntInt): scala.Unit = if (b == 0) out.collectElement(TupleIntInt.of(p.f0, 1))
		}, 1, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {})).addInput(pageAttributes, false).addInput(visits_1_tupleized, true, false)
		val counts = new LabyNode[TupleIntInt, TupleIntInt]("counts", new GroupBy0Sum1TupleIntInt, 1, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint[ElementOrEvent[TupleIntInt]]() {})).addInput(clicksMapped, true, false)
		val notFirstDay = new LabyNode[Integer, java.lang.Boolean]("notFirstDay", new SingletonBagOperator[Integer, java.lang.Boolean]() {
			override def pushInElement(e: Integer, logicalInputId: Int): scala.Unit = {
				super.pushInElement(e, logicalInputId)
				out.collectElement(!(e == 1))
			}
		}, 1, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Boolean]]() {})).addInput(day_2, true, false).setParallelism(1)
		val ifCond = new LabyNode[java.lang.Boolean, Unit]("ifCond", new ConditionNode(Array[Int](2, 3), Array[Int](3)), 1, new Always0[java.lang.Boolean](1), booleanSer, TypeInformation.of(new TypeHint[ElementOrEvent[Unit]]() {})).addInput(notFirstDay, true, false).setParallelism(1)
		// -- then branch   BB 2
		// The join of joinedYesterday is merged into this operator
		val diffs = new LabyNode[TupleIntInt, Integer]("diffs", new OuterJoinTupleIntInt[Integer]() {
			override protected def inner(b: Int, p: TupleIntInt): scala.Unit = out.collectElement(Math.abs(b - p.f1))

			override

			protected def right(p: TupleIntInt): scala.Unit = out.collectElement(p.f1)

			override

			protected def left(b: Int): scala.Unit = out.collectElement(b)
		}, 2, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(yesterdayCounts_2, false, true).addInput(counts, false, true)
		val sumCombiner = new LabyNode[Integer, Integer]("sumCombiner", new SumCombiner, 2, new Forward[Integer](para), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(diffs, true, false)
		val sum = new LabyNode[Integer, Integer]("sum", new Sum, 2, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(sumCombiner, true, false).setParallelism(1)
		val printSum = new LabyNode[Integer, Unit]("printSum", new CFAwareFileSink(pref + "out/diff_"), 2, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Unit]]() {})).addInput(day_2, false, true).addInput(sum, true, false).setParallelism(1)
		// -- end of then branch   BB 3
		// (We "optimize away" yesterdayCounts_3, since it would be an IdMap)
		yesterdayCounts_2.addInput(counts, false, true)
		val day_3 = new LabyNode[Integer, Integer]("day_3", new IncMap, 3, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[Integer]]() {})).addInput(day_2, false, false).setParallelism(1)
		day_2.addInput(day_3, false, true)
		val notLastDay = new LabyNode[Integer, java.lang.Boolean]("notLastDay", new SmallerThan(args(1).toInt + 1), 3, new Always0[Integer](1), integerSer, TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Boolean]]() {})).addInput(day_3, true, false).setParallelism(1)
		val exitCond = new LabyNode[java.lang.Boolean, Unit]("exitCond", new ConditionNode(1, 4), 3, new Always0[java.lang.Boolean](1), booleanSer, TypeInformation.of(new TypeHint[ElementOrEvent[Unit]]() {})).addInput(notLastDay, true, false).setParallelism(1)
		// -- Iteration ends here   BB 4
		// Itt nincs semmi operator. (A kiirast a BB 2-ben csinaljuk.)
		LabyNode.translateAll()
		env.execute
	}
}
