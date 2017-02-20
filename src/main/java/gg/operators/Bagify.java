package gg.operators;

import gg.ElementOrEvent;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;

/**
 * Create a bag from a DataStream, at the beginning of the program execution.
 */
public class Bagify<T>
        extends AbstractStreamOperator<ElementOrEvent<T>>
        implements OneInputStreamOperator<T, ElementOrEvent<T>>, Serializable {

    private byte subpartitionId;

    private final static int outCflSize = 1; // always 1, because this happens in the first basic block at the beginning of the program

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<T>>> output) {
        super.setup(containingTask, config, output);

        subpartitionId = (byte)getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, streamRecord.getValue()), 0));
    }


    @Override
    public void open() throws Exception {
        super.open();

        ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, outCflSize);
        output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event), 0));
    }

    @Override
    public void close() throws Exception {
        super.close();

        ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, outCflSize);
        output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event), 0));
    }
}
