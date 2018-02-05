package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.java.tuple.Tuple2;

public class GraphSink extends FileSink<Tuple2<Integer, Integer>> {

    public GraphSink(String path) {
        super(path);
    }

    @Override
    protected void print(Tuple2<Integer, Integer> e) {
        writer.println(e.f0 + " " + e.f1);
    }
}
