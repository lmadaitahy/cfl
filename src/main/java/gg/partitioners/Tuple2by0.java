package gg.partitioners;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class Tuple2by0<T> extends Partitioner<Tuple2<Integer, T>> {

    public Tuple2by0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(Tuple2<Integer, T> elem, short subpartitionId) {
        return (short)(elem.f0 % targetPara);
    }
}
