package inputgen;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Random;

/**
 * Creates numDays smaller graphs from a large graph, by filtering out edges randomly
 */
public class PageRankInputGen {

    public static void main(String[] args) throws Exception {
        final String pref = args[0] + "/";

        generate(pref, Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    }

    // Note: clicksPerDayRatio is actually the reciprocal, so 10 means every 10th edge is chosen
    public static void generate(String pref, int numDays, int clicksPerDayRatio) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String fullGraphPath = pref + "/fullGraph";
        String inputPath = pref + "/input";

        DataSet<Tuple2<Integer, Integer>> fullGraph = env.readCsvFile(fullGraphPath)
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .types(Integer.class, Integer.class);

        int day = 0;
        final int blockSize = 63;
        for (int i = 0; i < numDays / blockSize; i++) {
            for (int j = 0; j < blockSize; j++) {
                doBlock(fullGraph, clicksPerDayRatio, inputPath, day);
                day++;
            }
            env.execute();
        }
        for (int j = 0; j < numDays % blockSize; j++) {
            doBlock(fullGraph, clicksPerDayRatio, inputPath, day);
            day++;
        }
        env.execute();
    }

    private static void doBlock(DataSet<Tuple2<Integer, Integer>> fullGraph, int clicksPerDayRatio, String inputPath, Integer day) {
        DataSet<Tuple2<Integer, Integer>> filtered = fullGraph.filter(new RichFilterFunction<Tuple2<Integer, Integer>>() {

            Random rnd;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                rnd = new Random();
            }

            @Override
            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                return rnd.nextInt(clicksPerDayRatio) == 0;
            }
        });

        filtered.writeAsCsv(inputPath + "/" + day.toString(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);
    }
}
