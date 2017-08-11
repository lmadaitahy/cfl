package gg.operators;

import gg.BagID;
import gg.CFLManager;
import gg.ElementOrEvent;
import gg.partitioners.Partitioner;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;

/**
 * Create a bag from a DataStream, at the beginning of the program execution.
 * It prepends a start event and appends an end event.
 */
public class Bagify<T>
        extends AbstractStreamOperator<ElementOrEvent<T>>
        implements OneInputStreamOperator<T, ElementOrEvent<T>>, Serializable {

    private short subpartitionId;

    private final static int outCflSize = 1; // always 1, because this happens in the first basic block at the beginning of the program

    private Partitioner<T> partitioner;
    private boolean[] sentStart;

    private int opID = -1;

    private int numElements = -1;

    private CFLManager cflMan;

    private StreamRecord<T> reuseStreamRecord = new StreamRecord<>(null);
    private ElementOrEvent<T> reuseEleOrEvent = new ElementOrEvent<>();

    public Bagify(Partitioner<T> partitioner, int opID) {
        this.partitioner = partitioner;
        this.opID = opID;
    }

    public void setPartitioner(Partitioner<T> partitioner) {
        assert partitioner != null;
        assert this.partitioner == null; // Ez pl. olyankor durranhat el, ha ketszer akarnank hasznalni a jobban. Ilyenkor inkabb be kell rakni egy kozbulso LabyNode-ot
        this.partitioner = partitioner;
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<T>>> output) {
        super.setup(containingTask, config, output);

        subpartitionId = (short)getRuntimeContext().getIndexOfThisSubtask();
        sentStart = new boolean[partitioner.targetPara];

        cflMan = CFLManager.getSing();
    }



    @Override
    public void processElement(StreamRecord<T> e) throws Exception {
        numElements++;
        short part = partitioner.getPart(e.getValue(), subpartitionId);
        // (ez a logika ugyanez a BagOperatorHost-ban)
        if (!sentStart[part]) {
            sentStart[part] = true;
            ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, partitioner.targetPara, new BagID(outCflSize, opID));
            output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, (byte)-1, part), 0));
        }

        output.collect(reuseStreamRecord.replace(reuseEleOrEvent.replace(subpartitionId, e.getValue(), (byte)-1, part), 0));
    }


    @Override
    public void open() throws Exception {
        super.open();

        for(int i=0; i<sentStart.length; i++)
            sentStart[i] = false;

        numElements = 0;
    }

    @Override
    public void close() throws Exception {
        super.close();

        cflMan.producedLocal(new BagID(outCflSize, opID), new BagID[]{}, numElements, getRuntimeContext().getNumberOfParallelSubtasks(),subpartitionId,opID);

        for(short i=0; i<sentStart.length; i++) {
            if (sentStart[i]) {
                ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, partitioner.targetPara, new BagID(outCflSize, opID));
                output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, (byte) -1, i), 0));
            }
        }
    }
}
