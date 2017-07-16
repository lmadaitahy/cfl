package gg.operators;

/**
 * Don't forget to set the parallelism to 1!
 */
public class Or extends BagOperator<Boolean, Boolean> {

    private boolean result;

    @Override
    public void openOutBag() {
        super.openOutBag();
        result = false;
    }

    @Override
    public void pushInElement(Boolean e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        result = result || e;
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        out.collectElement(result);
        out.closeBag();
    }
}
