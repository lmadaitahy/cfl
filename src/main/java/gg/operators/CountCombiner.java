package gg.operators;

public class CountCombiner<T> extends BagOperator<T, Integer> {

    private int count = -1000000000;

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        count = 0;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        count += 1;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (count != 0) { // asszem fontos, hogy csak akkor kuldjunk, ha jott input elem
            out.collectElement(count);
        }
        out.closeBag();
    }
}
