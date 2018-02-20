package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GroupBy0Min1 extends BagOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupBy0Min1.class);

    private HashMap<Integer, Integer> hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new HashMap<>();
    }

    @Override
    public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
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
            out.collectElement(Tuple2.of((Integer)e.getKey(), (Integer)e.getValue()));
        }
        hm = null;
        out.closeBag();
    }
}
