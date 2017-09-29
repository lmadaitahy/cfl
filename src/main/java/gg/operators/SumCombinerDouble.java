package gg.operators;

/**
 * Sums the elements in the bag. (non-grouped)
 * This can be used both as a combiner and a final reduce step, it just depends on its parallelism. (But note that it doesn't emit if the sum is 0.)
 */
public class SumCombinerDouble extends BagOperator<Double, Double> {

    private double sum = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        sum = 0;
    }

    @Override
    public void pushInElement(Double e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        sum += e;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (sum != 0) {
            out.collectElement(sum);
        }
        out.closeBag();
    }
}
