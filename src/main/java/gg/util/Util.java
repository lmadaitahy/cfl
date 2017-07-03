package gg.util;

import gg.ElementOrEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Util {

	private static final Logger LOG = LoggerFactory.getLogger(Util.class);


	public static <T> TypeInformation<ElementOrEvent<T>> tpe() {
		return TypeInformation.of((Class<ElementOrEvent<T>>)(Class)ElementOrEvent.class);
	}


	public static DataStream<Tuple2<Integer, Integer>> getGraph(StreamExecutionEnvironment env, String[] args) {
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

		final ParameterTool params = ParameterTool.fromArgs(args);

		DataStream<Tuple2<Integer, Integer>> edgesStream;

		String file = params.get("edges");
		if (file != null) {
			LOG.info("Reading input from file " + file);
			edgesStream = env.createInput(new TupleCsvInputFormat<>(new Path(file),"\n", "\t", new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class))),
					new TupleTypeInfo<Tuple2<Integer, Integer>>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class)));
		} else {
			LOG.info("No input file given. Using built-in dataset.");
			edgesStream = env.fromCollection(Arrays.asList(edgesNB0));
		}

		return edgesStream.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
				out.collect(value);
				out.collect(Tuple2.of(value.f1, value.f0));
			}
		});
	}
}
