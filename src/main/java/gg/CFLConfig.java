package gg;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class CFLConfig implements Serializable {
    private static CFLConfig sing = new CFLConfig();

    public static CFLConfig getInstance() {
        return sing;
    }

    private CFLConfig() {}


    // Ezt be kell allitani meg a KickoffSource letrehozasa elott, mert az elrakja a ctorban.
    // Tovabba a job elindulasakor mar ennek be kell lennie allitva (vagyis nem lehetne a KickoffSource setupjaban bealltani ezt itt),
    // mert a BagOperatorHost setupjaban szukseg van ra, es a setupok sorrendje nem determinisztikus.
    public int terminalBBId = -1;

    public int numToSubscribe = -1;

    public void setNumToSubscribe() {
        int totalPara = 0;
        for (DataStream<?> ds: DataStream.btStreams) {
            totalPara += ds.getParallelism();
        }
        this.numToSubscribe = totalPara;
        DataStream.btStreams.clear();
    }




    public static final boolean vlog = false;

    public static final boolean logStartEnd = true;
}
