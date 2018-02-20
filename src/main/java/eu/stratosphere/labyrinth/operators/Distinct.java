package eu.stratosphere.labyrinth.operators;

import java.util.HashSet;

public class Distinct<T> extends BagOperator<T,T> {

    private HashSet<T> set;

    @Override
    public void openOutBag() {
        super.openOutBag();
        set = new HashSet<T>();
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (set.add(e)) {
            out.collectElement(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        set = null;
        out.closeBag();
    }
}
