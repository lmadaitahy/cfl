package gg;

import gg.operators.BagIdMap;
import gg.operators.Bagify;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CFLProba1 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};

		//env.readTextFile("~/Dropbox/cfl-data/wordcount.txt");

//		DataStream<ElementOrEvent<String>> input = env.fromElements(
//				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, 1)),
//				new ElementOrEvent<String>((byte)0, "alma"),
//				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, 1))
//		);


		DataStream<ElementOrEvent<String>> input =
				env.fromCollection(Arrays.asList(words))
						.transform("bagify", TypeInformation.of((Class<ElementOrEvent<String>>)(Class)ElementOrEvent.class), new Bagify<>());

		//System.out.println(input.getParallelism());

		DataStream<ElementOrEvent<String>> output = input
				.setConnectionType(new gg.partitioners.RoundRobin<String>())
				.transform("id-map",input.getType(),
				new BagOperatorHost<>(new BagIdMap<>(), 0, 0, 4));

		output.print();
		env.execute();
	}
}
