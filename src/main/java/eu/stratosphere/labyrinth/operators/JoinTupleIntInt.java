package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.SerializedBuffer;
import eu.stratosphere.labyrinth.util.TupleIntInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Joins (key,b) (build) with (key,c) (probe), giving (b, key, c) to the udf.
 * The first input is the build side.
 */
public abstract class JoinTupleIntInt<OUT> extends BagOperator<TupleIntInt, OUT> implements ReusingBagOperator {

    private static final Logger LOG = LoggerFactory.getLogger(JoinTupleIntInt.class);

    private Int2ObjectOpenHashMap<IntArrayList> ht;
    private SerializedBuffer<TupleIntInt> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        probeBuffered = new SerializedBuffer<>(new TupleIntInt.TupleIntIntSerializer());
        buildDone = false;
        probeDone = false;
        reuse = false;
    }

    @Override
    public void signalReuse() {
        reuse = true;
    }

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);

        if (logicalInputId == 0) {
            // build side
            if (!reuse) {
                ht = new Int2ObjectOpenHashMap<>(8192);
            }
        }
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            IntArrayList l = ht.get(e.f0);
            if (l == null) {
                l = new IntArrayList(2);
                l.add(e.f1);
                ht.put(e.f0,l);
            } else {
                l.add(e.f1);
            }
        } else { // probe side
            if (!buildDone) {
                probeBuffered.add(e);
            } else {
                probe(e);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (inputId == 0) { // build side
            assert !buildDone;
            //LOG.info("Build side finished");
            buildDone = true;
            for (TupleIntInt e: probeBuffered) {
                probe(e);
            }
            if (probeDone) {
                out.closeBag();
            }
        } else { // probe side
            assert inputId == 1;
            assert !probeDone;
            //LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                out.closeBag();
            }
        }
    }

    private void probe(TupleIntInt p) {
        IntArrayList l = ht.get(p.f0);
        if (l != null) {
            for (int b: l) {
                udf(b, p);
            }
        }
    }

    // b is the `value' in the build-side, p is the whole probe-side record (the key is p.f0)
    protected abstract void udf(int b, TupleIntInt p); // Uses out
}
