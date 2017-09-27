package gg.partitioners;

import gg.ElementOrEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * This just looks into the event to tell the result chosen by our manual partitioner to Flink.
 */
public class FlinkPartitioner<T> extends StreamPartitioner<ElementOrEvent<T>> {

    public static final short BROADCAST = -1;

    private final int[] arr = new int[1];
    private int[] broadcastArr = null;

    @Override
    public StreamPartitioner<ElementOrEvent<T>> copy() {
        return new FlinkPartitioner<T>();
    }

    @Override
    public int[] selectChannels(SerializationDelegate<StreamRecord<ElementOrEvent<T>>> record, int numChannels) {
        short targetPart = record.getInstance().getValue().targetPart;
        if (targetPart != BROADCAST) {
            arr[0] = targetPart;
            return arr;
        } else {
            return broadcast(numChannels);
        }
    }

    private int[] broadcast(int numChannels) {
        if (broadcastArr == null) {
            broadcastArr = new int[numChannels];
            for (int i=0; i<numChannels; i++) {
                broadcastArr[i] = i;
            }
        }
        assert broadcastArr.length == numChannels;
        return broadcastArr;
    }

    @Override
    public String toString() {
        return "FlinkPartitioner";
    }
}
