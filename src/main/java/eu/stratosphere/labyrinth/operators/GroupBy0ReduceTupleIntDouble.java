package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.TupleIntDouble;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.function.Consumer;

public abstract class GroupBy0ReduceTupleIntDouble extends BagOperator<TupleIntDouble, TupleIntDouble> {

    protected Int2DoubleOpenHashMap hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new Int2DoubleOpenHashMap();
        hm.defaultReturnValue(Double.MIN_VALUE);
    }

    @Override
    public void pushInElement(TupleIntDouble e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        double g = hm.putIfAbsent(e.f0, e.f1);
        if (g != hm.defaultReturnValue()) {
            reduceFunc(e, g);
        }
    }

    protected abstract void reduceFunc(TupleIntDouble e, double g);

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        hm.int2DoubleEntrySet().fastForEach(new Consumer<Int2DoubleMap.Entry>() {
            @Override
            public void accept(Int2DoubleMap.Entry e) {
                out.collectElement(TupleIntDouble.of(e.getIntKey(), e.getDoubleValue()));
            }
        });

        out.closeBag();
    }
}
