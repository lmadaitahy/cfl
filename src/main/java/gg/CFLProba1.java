package gg;

import gg.operators.BagIdMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CFLProba1 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};

		DataStream<ElementOrEvent<String>> input = env.fromElements(
				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, 1)),
				new ElementOrEvent<String>((byte)0, "alma"),
				new ElementOrEvent<String>((byte)0, new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, 1))
		);

		DataStream<ElementOrEvent<String>> output = input
				.setConnectionType(new gg.partitioners.RoundRobin<String>())
				.transform("id-map",input.getType(),
				new BagOperatorHost<>(new BagIdMap<>(), 0, 0, 1));

		output.print();
		env.execute();
	}
}
