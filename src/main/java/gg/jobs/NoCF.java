package gg.jobs;

import gg.BagOperatorHost;
import gg.CFLConfig;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.LabyNode;
import gg.LabySource;
import gg.operators.AssertBagEquals;
import gg.operators.Bagify;
import gg.operators.IdMap;
import gg.operators.Print;
import gg.partitioners.Always0;
import gg.partitioners.RoundRobin;
import gg.util.Nothing;
import gg.util.TupleIntInt;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Arrays;

public class NoCF {

	private static TypeSerializer<String> stringSer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		CFLConfig.getInstance().terminalBBId = 0;
		KickoffSource kickoffSrc = new KickoffSource(0);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

        final int para = env.getParallelism();

		String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};

		LabySource<String> input = new LabySource<>(env.fromCollection(Arrays.asList(words)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}));

		//System.out.println(input.getParallelism());

		LabyNode<String, String> output =
				new LabyNode<>("id-map", new IdMap<>(), 0, new RoundRobin<>(para), stringSer, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}))
				.addInput(input, true);

		LabyNode<String, Nothing> sink =
				new LabyNode<String, Nothing>(
						"assert",
						new AssertBagEquals<>("alma", "korte", "alma", "b", "b", "b", "c", "d", "d"),
						0,
						new Always0<String>(1), stringSer, TypeInformation.of(new TypeHint<ElementOrEvent<Nothing>>(){}))
				.addInput(output, true, false)
				.setParallelism(1);

		LabyNode<String, Unit> pr =
				new LabyNode<>(
						"print",
						new Print<>("foo"),
						0,
						new Always0<>(1),
						stringSer,
						TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){})
				)
						.addInput(output, true, false)
						.setParallelism(1);

		LabyNode.translateAll();

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
