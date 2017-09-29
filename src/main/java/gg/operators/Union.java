package gg.operators;

public class Union<T> extends BagOperator<T, T> {

    private final boolean[] open = new boolean[2];

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        open[0] = true;
        open[1] = true;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        out.collectElement(e);
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert open[inputId];
        open[inputId] = false;
        if (!open[0] && !open[1]) {
            out.closeBag();
        }
    }
}
