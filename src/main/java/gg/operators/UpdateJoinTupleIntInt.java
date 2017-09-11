package gg.operators;

import gg.util.SerializedBuffer;
import gg.util.TupleIntInt;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

public class UpdateJoinTupleIntInt extends BagOperator<TupleIntInt, TupleIntInt> {

//    private static final Logger LOG = LoggerFactory.getLogger(Join.class);

    private Int2IntOpenHashMap ht; // TODO: use fastutils
    private SerializedBuffer<TupleIntInt> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private int buildCnt;
    private int probeCnt;

    @Override
    public void openOutBag() {
        super.openOutBag();
        ht = new Int2IntOpenHashMap(4096);
        ht.defaultReturnValue(Integer.MIN_VALUE);
        probeBuffered = new SerializedBuffer<>(new TupleIntInt.TupleIntIntSerializer());
        buildDone = false;
        probeDone = false;
        buildCnt = 0;
        probeCnt = 0;
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            buildCnt++;
            assert !buildDone;
            assert ht.get(e.f0) == ht.defaultReturnValue(); // should be a primary key
            ht.put(e.f0, e.f1);
        } else { // probe side
            probeCnt++;
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
//            LOG.info("Build side finished");
            buildDone = true;
            for (TupleIntInt e: probeBuffered) {
                probe(e);
            }
            if (probeDone) {
                emitAndClose();
            }
        } else { // probe side
            assert inputId == 1;
            assert !probeDone;
//            LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                emitAndClose();
            }
        }
    }

    private void probe(TupleIntInt e) {
        int r = ht.replace(e.f0, e.f1);
        assert r != ht.defaultReturnValue(); // Let's not allow for insertions for the moment.
    }

    private void emitAndClose() {
        ht.int2IntEntrySet().fastForEach(new Consumer<Int2IntMap.Entry>() {
            @Override
            public void accept(Int2IntMap.Entry e) {
                out.collectElement(TupleIntInt.of(e.getIntKey(), e.getIntValue()));
            }
        });
//        LOG.info("buildCnt: " + buildCnt + ", probeCnt: " + probeCnt);
        out.closeBag();
    }
}
