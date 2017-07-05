package gg.operators;

import gg.util.TupleIntInt;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GroupBy0Min1TupleIntInt extends BagOperator<TupleIntInt, TupleIntInt> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupBy0Min1TupleIntInt.class);

    private HashMap<Integer, Integer> hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new HashMap<>();
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        Integer g = hm.get(e.f0);
        if (g == null) {
            hm.put(e.f0, e.f1);
        } else {
            if (e.f1 < g) {
                hm.replace(e.f0, e.f1);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        for (HashMap.Entry e: hm.entrySet()) {
            out.collectElement(TupleIntInt.of((int)e.getKey(), (int)e.getValue()));
        }
        out.closeBag();
    }
}
