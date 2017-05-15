package gg.partitioners;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Partition bag of Tuple2s by f0.
 */
public class Tuple2by0 extends EventBroadcast<Tuple2<Integer, Integer>> {
    @Override
    protected int selectForElement(Tuple2<Integer, Integer> elem, int numberOfOutputChannels) {
        return elem.f0 % numberOfOutputChannels;
    }
}
