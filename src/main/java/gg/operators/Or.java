package gg.operators;

/**
 * Don't forget to set the parallelism to 1!
 */
public class Or extends BagOperator<Boolean, Boolean> {

    private boolean result;
    private boolean sent = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        result = false;
        sent = false;
    }

    @Override
    public void pushInElement(Boolean e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        result = result || e;
        if (result && !sent) {
            sent = true;
            out.collectElement(true);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (!sent) {
            assert !result;
            out.collectElement(false);
        }
        out.closeBag();
    }
}
