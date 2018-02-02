package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.SerializedBuffer;
import org.apache.flink.api.common.typeutils.TypeSerializer;

abstract public class OpWithSingletonSide<IN, OUT> extends OpWithSideInput<IN, OUT> {

    private IN sideSing;

    public OpWithSingletonSide(TypeSerializer<IN> inSer) {
        super(inSer);
    }

    @Override
    public void openOutBag() {
        super.openOutBag();
        sideSing = null;
    }

    @Override
    public void closeInBag(int inputId) {
        if (inputId == 0) {
            // side
            for (IN e: sideBuffered) {
                assert sideSing == null;
                sideSing = e;
            }
        }
        super.closeInBag(inputId);
    }

    @Override
    protected void pushInElementWithSide(IN e, SerializedBuffer<IN> side) {
        assert this.sideSing != null;
        pushInElementWithSingletonSide(e, this.sideSing);
    }

    abstract protected void pushInElementWithSingletonSide(IN e, IN side);
}
