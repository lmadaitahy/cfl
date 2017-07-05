package gg.partitioners;

import gg.util.TupleIntInt;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class TupleIntIntBy0 extends Partitioner<TupleIntInt> {

    public TupleIntIntBy0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(TupleIntInt elem, short subpartitionId) {
        return (short)(elem.f0 % targetPara);
    }
}
