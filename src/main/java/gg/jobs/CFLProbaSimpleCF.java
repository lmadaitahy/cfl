package gg.jobs;

import gg.BagOperatorHost;
import gg.CondOutputSelector;
import gg.ElementOrEvent;
import gg.operators.ConditionNode;
import gg.operators.IdMap;
import gg.operators.Bagify;
import gg.operators.IncMap;
import gg.operators.SmallerThan;
import gg.util.Unit;
import gg.util.Util;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.xml.Elem;

import java.util.Arrays;

public class CFLProbaSimpleCF {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);

		Integer[] input = new Integer[]{1};

		DataStream<ElementOrEvent<Integer>> inputBag =
				env.fromCollection(Arrays.asList(input))
						.transform("bagify",
								Util.tpe(), new Bagify<>());

		IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(5000); //todo: majd nagyra allitani miutan van sajat termination

		SplitStream<ElementOrEvent<Integer>> incedSplit = it
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("inc-map",inputBag.getType(),
						new BagOperatorHost<>(new IncMap(), 0, 0)
								.out(0,1,false)
								.out(1,0,false)
								.out(2,0,true))
				.split(new CondOutputSelector<>());

		it.closeWith(incedSplit.select("0"));

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("smaller-than",Util.tpe(),
						new BagOperatorHost<>(new SmallerThan(10), 0, 0)
								.out(0,0,true));

		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
				.setConnectionType(new gg.partitioners.Forward<>())
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<Boolean, Unit>(new ConditionNode(0,1), 0, 0));

		// Edge going out of the loop
		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

		output.print();
		env.execute();
	}
}
