package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.TupleIntInt;

public class GraphSinkTupleIntInt extends FileSink<TupleIntInt> {

    public GraphSinkTupleIntInt(String path) {
        super(path);
    }

    @Override
    protected void print(TupleIntInt e) {
        writer.println(e.f0 + " " + e.f1);
    }
}
