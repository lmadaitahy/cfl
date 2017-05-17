package gg.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Don't forget to set the parallelism to 1!
 */
public class NonEmpty<T> extends BagOperator<T, Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(NonEmpty.class);

    private static final int closedNum = -1000;

    private int num = closedNum;
    private boolean sent = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        assert num == closedNum;
        num = 0;
        sent = false;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        num++;
        if (!sent) {
            out.collectElement(true);
            sent = true;
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (num == 0) {
            out.collectElement(false);
        }
        num = closedNum;
        out.closeBag();
    }
}
