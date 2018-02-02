package eu.stratosphere.labyrinth.operators;

/**
 * Sums the elements in the bag. (non-grouped)
 * Parallelism should be 1. (there is a separate SumCombiner)
 */
public class Sum extends BagOperator<Integer, Integer> {

    private int sum = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        assert host.subpartitionId == 0; // Parallelism should be 1.
        sum = 0;
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        sum += e;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        out.collectElement(sum);
        out.closeBag();
    }
}
