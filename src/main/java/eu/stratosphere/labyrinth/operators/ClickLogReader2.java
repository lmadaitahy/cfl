package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.TupleIntInt;

public class ClickLogReader2 extends CFAwareFileSource<TupleIntInt> {

    public ClickLogReader2(String baseName) {
        super(baseName);
    }

    @Override
    protected TupleIntInt parseLine(String line) {
        String[] arr = line.split("\t");
        return TupleIntInt.of(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]));
    }
}
