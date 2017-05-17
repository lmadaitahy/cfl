package gg.operators;

import gg.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Print<T> extends BagOperator<T, Unit> {

    private static final Logger LOG = LoggerFactory.getLogger(Print.class);

    private final String name;

    public Print(String name) {
        this.name = name;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        LOG.info("Print("+name+") element: " + e);
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        out.closeBag();
    }
}
