package gg.operators;

import gg.util.TupleIntInt;

public class GroupBy0Min1TupleIntInt extends GroupBy0ReduceTupleIntInt {

    @Override
    protected void reduceFunc(TupleIntInt e, int g) {
        if (e.f1 < g) {
            hm.replace(e.f0, e.f1);
        }
    }
}
