package gg.partitioners;

import gg.ElementOrEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * This just looks into the event to tell the result chosen by our manual partitioner to Flink.
 */
public class FlinkPartitioner<T> extends StreamPartitioner<ElementOrEvent<T>> {

    private final int[] arr = new int[1];

    @Override
    public StreamPartitioner<ElementOrEvent<T>> copy() {
        return new FlinkPartitioner<T>();
    }

    @Override
    public int[] selectChannels(SerializationDelegate<StreamRecord<ElementOrEvent<T>>> record, int numChannels) {
        arr[0] = record.getInstance().getValue().targetPart;
        return arr;
    }

    @Override
    public String toString() {
        return "FlinkPartitioner";
    }
}
