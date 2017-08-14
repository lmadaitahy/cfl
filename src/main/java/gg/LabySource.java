package gg;

import gg.operators.Bagify;
import gg.util.Nothing;
import gg.util.Util;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LabySource<T> extends AbstractLabyNode<Nothing, T> {

    public final int bbId;
    public final int opID;

    private final DataStream<T> inputStream;

    public final Bagify<T> bagify;

    private DataStream<ElementOrEvent<T>> flinkStream;

    public LabySource(DataStream<T> inputStream, int bbId) {
        assert bbId == 0; // ezt majd akkor lehet kivenni, hogyha megcsinaltam, hogy a Bagify tudjon tobbszor kuldeni (ez ugyebar kelleni fog a PageRank-hez)
        this.bbId = bbId;
        this.opID = labyNodes.size();
        this.inputStream = inputStream;
        bagify = new Bagify<>(null, opID);
        labyNodes.add(this);
    }

    @Override
    protected List<AbstractLabyNode<?, Nothing>> getInputNodes() {
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
                .setConnectionType(new gg.partitioners.FlinkPartitioner<>());
    }
}
