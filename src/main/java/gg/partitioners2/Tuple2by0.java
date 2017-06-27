package gg.partitioners2;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class Tuple2by0 extends Partitioner<Tuple2<Integer, Integer>> {

    public Tuple2by0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(Tuple2<Integer, Integer> elem) {
        return (short)(elem.f0 % targetPara);
    }
}
