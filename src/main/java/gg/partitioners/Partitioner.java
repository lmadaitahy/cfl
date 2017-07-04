package gg.partitioners;

import java.io.Serializable;

public abstract class Partitioner<T> implements Serializable {

    public short targetPara = 0;

    /**
     * Ugyebar a targetPara a kov operator parallelismje!
     */
    public Partitioner(short targetPara) {
        this.targetPara = targetPara;
    }

    public Partitioner(int targetPara) {
        this.targetPara = (short)targetPara;
    }

    public abstract short getPart(T e, short subpartitionId);
}
