package eu.stratosphere.labyrinth;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class CFLConfig implements Serializable {
    private static CFLConfig sing = new CFLConfig();

    public static CFLConfig getInstance() {
        return sing;
    }

    private CFLConfig() {}

    // This has to be set before creating the KickoffSource, which takes and stores it in the constructor.
    // Furthermore it has to be set when the job starts (i.e. it can't be set during KickoffSource setup)
    // because it is needed during BagOperatorHost setup and the order of the setups is not deterministic.
    public int terminalBBId = -1;

    public int numToSubscribe = -1;

    public void setNumToSubscribe() {
        int totalPara = 0;
        for (DataStream<?> ds: DataStream.btStreams) {
            totalPara += ds.getParallelism();
        }
        DataStream.btStreams.clear();
        setNumToSubscribe(totalPara);
    }

    public void setNumToSubscribe(int totalPara) {
        this.numToSubscribe = totalPara;
    }


    public boolean reuseInputs = true;



    public static final boolean vlog = false;

    public static final boolean logStartEnd = false;
}
