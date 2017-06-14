package gg.partitioners2;

public abstract class Partitioner<T> {

    public short targetPara = 0;

    /**
     * Ugyebar a targetPara a kov operator parallelismje!
     */
    public Partitioner(short targetPara) {
        this.targetPara = targetPara;
    }

    public abstract short getPart(T e);
}
