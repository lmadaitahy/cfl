package nocfl;

import gg.util.TupleIntInt;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;


// Ezt most egyelore felbehagyom, es inkabb a wordcount-ot rakom be az inner loop helyett


public class NonFixpoint {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int days = 2; // 365
		for (int day = 1; day <= days; day++) {

			DataSet<Integer> clicks = env.readCsvFile("clickLog" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Integer.class)
					.map(new MapFunction<Tuple1<Integer>, Integer>() {
						@Override
						public Integer map(Tuple1<Integer> value) throws Exception {
							return value.f0;
						}
					});



			

		}

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
