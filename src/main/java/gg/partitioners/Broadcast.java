package gg.partitioners;

public class Broadcast<T> extends Partitioner<T> {

    public Broadcast(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(T e, short subpartitionId) {
        return FlinkPartitioner.BROADCAST;
    }
}
