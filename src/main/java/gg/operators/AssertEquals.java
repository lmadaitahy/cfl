package gg.operators;

import gg.util.TestFailedException;
import gg.util.Unit;

import java.io.Serializable;

// !!! Don't forget to set the parallelism to 1 !!!
public class AssertEquals<IN> extends SingletonBagOperator<IN, Unit> implements Serializable {

    private final IN x;

    public AssertEquals(IN x) {
        this.x = x;
    }

    @Override
    public void pushInElement(IN e) {
        super.pushInElement(e);
        System.out.println("AssertEqualsing");

        if (!e.equals(x)) {
            throw new TestFailedException("AssertEquals failed: got: " + e + ", expected " + x);
        }

        collector.collectElement(new Unit());
    }
}
