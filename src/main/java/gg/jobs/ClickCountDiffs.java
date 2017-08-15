package gg.jobs;

import gg.LabyNode;
import gg.LabySource;
import gg.partitioners.Forward;
import gg.util.TupleIntInt;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class ClickCountDiffs {

    private static TupleTypeInfo<Tuple2<Integer, Integer>> typeInfoTupleIntInt = new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class));

    private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<TupleIntInt> tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String pref = args[0] + "/";

        DataStream<TupleIntInt> pageAttributesStream = env.createInput(new TupleCsvInputFormat<Tuple2<Integer, Integer>>(
                new Path(pref + "in/pageAttributes.tsv"),"\n", "\t", typeInfoTupleIntInt), typeInfoTupleIntInt)
                .map(new MapFunction<Tuple2<Integer, Integer>, TupleIntInt>() {
            @Override
            public TupleIntInt map(Tuple2<Integer, Integer> value) throws Exception {
                return TupleIntInt.of(value.f0, value.f1);
            }
        });

        LabySource<TupleIntInt> pageAttributes = new LabySource<>(pageAttributesStream, 0);

        @SuppressWarnings("unchecked")
        LabySource<TupleIntInt> yesterdayCounts_1 = new LabySource<>(env.fromCollection(Collections.emptyList(), TypeInformation.of(TupleIntInt.class)), 0);

        //todo: set para to 1
        LabySource<Integer> day_1 = new LabySource<>(env.fromCollection(Collections.singletonList(1)), 0);

        // -- Iteration starts here --

        LabyNode<TupleIntInt, TupleIntInt> yesterdayCounts_2 =
                LabyNode.phi("yesterdayCounts_2", 1, new Forward<>(env.getParallelism()), tupleIntIntSer);

        LabyNode<Integer, Integer> day_2 =
                LabyNode.phi("day_2", 1, new Forward<>(1), integerSer);

        env.execute();
    }
}
