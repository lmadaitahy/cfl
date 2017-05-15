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

    @Override
    public void openOutBag() {
        super.openOutBag();
        assert num == closedNum;
        num = 0;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        num++;
        out.collectElement(true);
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        ////
        LOG.info("NonEmpty.closeInBag " + num);
        ////

        if (num == 0) {
            out.collectElement(false);
        }
        num = closedNum;
        out.closeBag();
    }
}
