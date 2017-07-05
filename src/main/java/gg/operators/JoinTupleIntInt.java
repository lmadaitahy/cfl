package gg.operators;

import gg.util.TupleIntInt;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class JoinTupleIntInt extends BagOperator<TupleIntInt, TupleIntInt> {

    private static final Logger LOG = LoggerFactory.getLogger(JoinTupleIntInt.class);

    private HashMap<Integer, ArrayList<TupleIntInt>> ht;
    private ArrayList<TupleIntInt> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    @Override
    public void openOutBag() {
        super.openOutBag();
        ht = new HashMap<>();
        probeBuffered = new ArrayList<>();
        buildDone = false;
        probeDone = false;
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            ArrayList<TupleIntInt> l = ht.get(e.f0);
            if (l == null) {
                l = new ArrayList<>();
                l.add(e);
                ht.put(e.f0,l);
            } else {
                l.add(e);
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
            LOG.info("Build side finished");
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
            LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                out.closeBag();
            }
        }
    }

    private void probe(TupleIntInt e) {
        ArrayList<TupleIntInt> l = ht.get(e.f0);
        if (l != null) {
            for (TupleIntInt b: l) {
                udf(b, e);
            }
        }
    }

    protected abstract void udf(TupleIntInt a, TupleIntInt b); // Uses out
}
