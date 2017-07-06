package gg.operators;

import gg.util.TupleIntInt;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.HashMap;
import java.util.function.Consumer;

public class GroupBy0Min1TupleIntInt extends BagOperator<TupleIntInt, TupleIntInt> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupBy0Min1TupleIntInt.class);

    private Int2IntOpenHashMap hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new Int2IntOpenHashMap();
        hm.defaultReturnValue(Integer.MIN_VALUE);
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

//        int g = hm.get(e.f0);
//        if (g == hm.defaultReturnValue()) {
//            hm.put(e.f0, e.f1);
//        } else {
//            if (e.f1 < g) {
//                hm.replace(e.f0, e.f1);
//            }
//        }

        int g = hm.putIfAbsent(e.f0, e.f1);
        if (g != hm.defaultReturnValue()) {
            if (e.f1 < g) {
                hm.replace(e.f0, e.f1);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

//        for (HashMap.Entry e: hm.entrySet()) {
//            out.collectElement(TupleIntInt.of((int)e.getKey(), (int)e.getValue()));
//        }

        hm.int2IntEntrySet().fastForEach(new Consumer<Int2IntMap.Entry>() {
            @Override
            public void accept(Int2IntMap.Entry e) {
                out.collectElement(TupleIntInt.of(e.getIntKey(), e.getIntValue()));
            }
        });

        out.closeBag();
    }
}
