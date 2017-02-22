package gg.jobs;

import gg.BagOperatorHost;
import gg.CFLManager;
import gg.ElementOrEvent;
import gg.KickoffSource;
import gg.operators.IdMap;
import gg.operators.Bagify;
import gg.util.Util;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Arrays;

public class CFLProba1 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(new KickoffSource(0)).addSink(new DiscardingSink<>());

		String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};

//		DataStream<ElementOrEvent<String>> input = env.fromElements(
//				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, 1)),
//				new ElementOrEvent<String>((byte)0, "alma"),
//				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, 1))
//		);


		DataStream<ElementOrEvent<String>> input =
				env.fromCollection(Arrays.asList(words))
						.transform("bagify", Util.tpe(), new Bagify<>());

		//System.out.println(input.getParallelism());

		DataStream<ElementOrEvent<String>> output = input
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("id-map",input.getType(),
				new BagOperatorHost<String, String>(
						new IdMap<>(), 0, new Integer[]{0}, true).out(0,0,true));

		output.print();
		env.execute();
	}
}
