package gg.partitioners;

import gg.util.TupleIntDouble;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class TupleIntDoubleBy0 extends Partitioner<TupleIntDouble> {

    public TupleIntDoubleBy0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(TupleIntDouble elem, short subpartitionId) {
        return (short)(elem.f0 % targetPara);
    }
}
