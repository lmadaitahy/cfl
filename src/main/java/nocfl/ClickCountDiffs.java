package nocfl;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


public class ClickCountDiffs {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		String pref = args[0] + "/";
		String yesterdayCountsTmpFilename = pref + "tmp/yesterdayCounts";

		DataSet<Tuple2<Integer, Integer>> pageAttributes = env.readCsvFile(pref + "in/pageAttributes.tsv")
				.fieldDelimiter("\t")
				.lineDelimiter("\n")
				.types(Integer.class, Integer.class);

		final int days = Integer.parseInt(args[1]); // 365
		for (int day = 1; day <= days; day++) {

			DataSet<Tuple1<Integer>> visits = env.readCsvFile(pref + "in/clickLog_" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Integer.class);

			DataSet<Integer> visitsFiltered = visits.join(pageAttributes).where(0).equalTo(0).with(new FlatJoinFunction<Tuple1<Integer>, Tuple2<Integer, Integer>, Integer>() {
				@Override
				public void join(Tuple1<Integer> first, Tuple2<Integer, Integer> second, Collector<Integer> out) throws Exception {
					if (second.f1.equals(0)) { // (Mondjuk a Labyrinth jobban ez egy kulon operator, szoval lehet, hogy itt is ugy kene)
						out.collect(first.f0);
					}
				}
			});

			DataSet<Tuple2<Integer, Integer>> counts = visitsFiltered.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Integer value) throws Exception {
					return Tuple2.of(value, 1);
				}
			}).groupBy(0).sum(1);

			if (day != 1) {

				DataSet<Tuple2<Integer, Integer>> yesterdayCounts = env.readCsvFile(yesterdayCountsTmpFilename).types(Integer.class, Integer.class);

//				DataSet<Tuple1<Integer>> diffs = counts.join(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple1<Integer>>() {
//					@Override
//					public Tuple1<Integer> join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second) throws Exception {
//						return Tuple1.of(Math.abs(first.f1 - second.f1));
//					}
//				});

				DataSet<Tuple1<Integer>> diffs = counts.fullOuterJoin(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple1<Integer>>() {

					Tuple2<Integer, Integer> nulla = Tuple2.of(0,0);

					@Override
					public Tuple1<Integer> join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second) throws Exception {
						if (first == null) {
							first = nulla;
						}
						if (second == null) {
							second = nulla;
						}
						return Tuple1.of(Math.abs(first.f1 - second.f1));
					}
				});

				diffs.sum(0).setParallelism(1).writeAsText(pref + "out/expected/diff_" + day, FileSystem.WriteMode.OVERWRITE);
			}

			counts.writeAsCsv(yesterdayCountsTmpFilename, FileSystem.WriteMode.OVERWRITE);

			env.execute();
		}
	}
}
