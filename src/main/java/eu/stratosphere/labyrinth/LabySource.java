package eu.stratosphere.labyrinth;

import eu.stratosphere.labyrinth.operators.Bagify;
import eu.stratosphere.labyrinth.util.Nothing;
import eu.stratosphere.labyrinth.util.Util;
import eu.stratosphere.labyrinth.partitioners.FlinkPartitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.ArrayList;
import java.util.List;

public class LabySource<T> extends AbstractLabyNode<T> {

    public final int bbId;
    public final int opID;

    private final DataStream<T> inputStream;

    public final Bagify<T> bagify;

    private SingleOutputStreamOperator<ElementOrEvent<T>> flinkStream;

    private int parallelism = -1;

    private final TypeInformation<ElementOrEvent<T>> typeInfo;

    public LabySource(DataStream<T> inputStream, int bbId, TypeInformation<ElementOrEvent<T>> typeInfo) {
        assert bbId == 0; // ezt majd akkor lehet kivenni, hogyha megcsinaltam, hogy a Bagify tudjon tobbszor kuldeni (bar ezt kozben maskepp csinalom, ld. CFAwareFileSource)
        this.bbId = bbId;
        this.opID = labyNodes.size();
        this.inputStream = inputStream;
        this.typeInfo = typeInfo;
        bagify = new Bagify<>(null, opID);
        labyNodes.add(this);
    }

    public LabySource<T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    protected List<AbstractLabyNode<?>> getInputNodes() {
        return new ArrayList<>();
    }

    @Override
    public DataStream<ElementOrEvent<T>> getFlinkStream() {
        return flinkStream;
    }

    @Override
    protected void translate() {
        flinkStream = inputStream
                .transform("bagify", Util.tpe(), bagify)
                .returns(typeInfo);

        if (parallelism != -1) {
            flinkStream = flinkStream.setParallelism(parallelism);
        }

        flinkStream = flinkStream.setConnectionType(new FlinkPartitioner<>());
    }
}
