package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.SerializedBuffer;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * This binary operator has a process method only for the second input, and the first input is available there from an Iterable.
 * It buffers up elements from the second input while the first input has not fully arrived.
 * The first input is the side input, and the second input is the main input.
 */
abstract public class OpWithSideInput<IN, OUT> extends BagOperator<IN, OUT> {

    protected SerializedBuffer<IN> sideBuffered;
    private SerializedBuffer<IN> mainBuffered;

    private boolean sideDone, mainDone;

    private final TypeSerializer<IN> inSer;


    public OpWithSideInput(TypeSerializer<IN> inSer) {
        this.inSer = inSer;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();
        sideBuffered = new SerializedBuffer<>(inSer);
        mainBuffered = new SerializedBuffer<>(inSer);
        sideDone = false;
        mainDone = false;
    }

    @Override
    public void pushInElement(IN e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) {
            // side
            assert !sideDone;
            sideBuffered.add(e);
        } else {
            // main
            if (!sideDone) {
                mainBuffered.add(e);
            } else {
                pushInElementWithSide(e, sideBuffered);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (inputId == 0) {
            // side
            assert !sideDone;
            sideDone = true;
            for (IN e: mainBuffered) {
                pushInElementWithSide(e, sideBuffered);
            }
            mainBuffered = null;
            if (mainDone) {
                out.closeBag();
            }
        } else {
            // main
            assert inputId == 1;
            mainDone = true;
            if (!sideDone) {
                // do nothing
            } else {
                out.closeBag();
            }
        }
    }

    abstract protected void pushInElementWithSide(IN e, SerializedBuffer<IN> side);
}
