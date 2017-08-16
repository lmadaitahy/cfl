package gg.operators;

/**
 * Sums the elements in the bag. (non-grouped)
 * This can be used both as a combiner and a final reduce step, it just depends on its parallelism.
 */
public class Sum extends BagOperator<Integer, Integer> {

    private int sum = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
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
    }
}
