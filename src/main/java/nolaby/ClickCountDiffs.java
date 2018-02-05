package nolaby;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClickCountDiffs {

	private static final Logger LOG = LoggerFactory.getLogger(ClickCountDiffs.class);

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		env.getConfig().enableObjectReuse();

		String pref = args[0] + "/";
		String yesterdayCountsTmpFilename = pref + "tmp/yesterdayCounts";

		DataSet<Tuple2<IntValue, IntValue>> pageAttributes = env.readCsvFile(pref + "in/pageAttributes.tsv")
				.fieldDelimiter("\t")
				.lineDelimiter("\n")
				.types(IntValue.class, IntValue.class);

		final int days = Integer.parseInt(args[1]); // 365
		for (int day = 1; day <= days; day++) {

			LOG.info("### Day " + day);

			DataSet<Tuple1<IntValue>> visits = env.readCsvFile(pref + "in/clickLog_" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(IntValue.class);

			DataSet<IntValue> visitsFiltered = visits.join(pageAttributes).where(0).equalTo(0).with(new FlatJoinFunction<Tuple1<IntValue>, Tuple2<IntValue, IntValue>, IntValue>() {

				IntValue zero = new IntValue(0);

				@Override
				public void join(Tuple1<IntValue> first, Tuple2<IntValue, IntValue> second, Collector<IntValue> out) throws Exception {
					if (second.f1.equals(zero)) { // (Mondjuk a Labyrinth jobban ez egy kulon operator, szoval lehet, hogy itt is ugy kene)
						out.collect(first.f0);
					}
				}
			});

			DataSet<Tuple2<IntValue, IntValue>> counts = visitsFiltered.map(new MapFunction<IntValue, Tuple2<IntValue, IntValue>>() {

				Tuple2<IntValue, IntValue> reuse = Tuple2.of(new IntValue(-1),new IntValue(1));

				@Override
				public Tuple2<IntValue, IntValue> map(IntValue value) throws Exception {
					reuse.f0 = value;
					return reuse;
				}
			}).groupBy(0).sum(1);

			if (day != 1) {

				DataSet<Tuple2<IntValue, IntValue>> yesterdayCounts = env.readCsvFile(yesterdayCountsTmpFilename).types(IntValue.class, IntValue.class);

				DataSet<Tuple1<IntValue>> diffs = counts.fullOuterJoin(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<IntValue,IntValue>, Tuple2<IntValue,IntValue>, Tuple1<IntValue>>() {

					Tuple2<IntValue, IntValue> nulla = Tuple2.of(new IntValue(0),new IntValue(0));

					Tuple1<IntValue> reuse = Tuple1.of(new IntValue(-1));

					@Override
					public Tuple1<IntValue> join(Tuple2<IntValue, IntValue> first, Tuple2<IntValue, IntValue> second) throws Exception {
						if (first == null) {
							first = nulla;
						}
						if (second == null) {
							second = nulla;
						}
						reuse.f0.setValue(Math.abs(first.f1.getValue() - second.f1.getValue()));
						return reuse;
					}
				});

				diffs.sum(0).map(new MapFunction<Tuple1<IntValue>, String>() {
					@Override
					public String map(Tuple1<IntValue> integerTuple1) throws Exception {
						return integerTuple1.f0.toString();
					}
				}).setParallelism(1).writeAsText(pref + "out/expected/diff_" + day, FileSystem.WriteMode.OVERWRITE);
			}

			counts.writeAsCsv(yesterdayCountsTmpFilename, FileSystem.WriteMode.OVERWRITE);

			env.execute();
		}
	}
}
