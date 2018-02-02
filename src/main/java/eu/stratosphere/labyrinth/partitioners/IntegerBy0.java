package eu.stratosphere.labyrinth.partitioners;

/**
 * Partition a bag of Integers by the whole element.
 */
public class IntegerBy0 extends Partitioner<Integer> {

    public IntegerBy0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(Integer elem, short subpartitionId) {
        return (short)(elem % targetPara);
    }
}
