package nocfl;

import gg.util.TestFailedException;
import gg.util.TupleIntInt;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Arrays;


// Ezt most egyelore felbehagyom, es inkabb a wordcount-ot rakom be az inner loop helyett


public class PageRankDiffs {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int days = 2; // 365
		for (int day = 1; day <= days; day++) {

			DataSet<TupleIntInt> clicks = env.readCsvFile("clickLog" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Integer.class, Integer.class)
					.map(new MapFunction<Tuple2<Integer, Integer>, TupleIntInt>() {
						@Override
						public TupleIntInt map(Tuple2<Integer, Integer> value) throws Exception {
							return TupleIntInt.of(value.f1, value.f1);
						}
					});

			DataSet<Integer> pages = clicks.map(new MapFunction<TupleIntInt, Integer>() {
				@Override
				public Integer map(TupleIntInt value) throws Exception {
					return value.f1;
				}
			}).distinct();

			long numPages = pages.count();
			double initWeight = 1.0d/numPages;
			DataSet<Tuple2<Integer, Double>> pr = pages.map(new MapFunction<Integer, Tuple2<Integer, Double>>() {
				@Override
				public Tuple2<Integer, Double> map(Integer id) throws Exception {
					return Tuple2.of(id, initWeight);
				}
			});

			

		}

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
