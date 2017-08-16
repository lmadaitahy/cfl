package gg.operators;

import gg.util.TupleIntInt;

public class GroupBy0Sum1TupleIntInt extends GroupBy0ReduceTupleIntInt {

    @Override
    protected void reduceFunc(TupleIntInt e, int g) {
        hm.replace(e.f0, e.f1 + g);
    }
}