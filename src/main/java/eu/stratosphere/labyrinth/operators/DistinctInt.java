package eu.stratosphere.labyrinth.operators;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class DistinctInt extends BagOperator<Integer,Integer> {

    private IntOpenHashSet set;

    @Override
    public void openOutBag() {
        super.openOutBag();
        set = new IntOpenHashSet();
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (set.add((int)e)) {
            out.collectElement(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        out.closeBag();
        set = null;
    }
}
