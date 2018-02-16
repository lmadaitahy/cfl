package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;

public abstract class JoinGeneric<IN, K> extends BagOperator<IN, Tuple2<IN, IN>> implements ReusingBagOperator {

	private HashMap<K, ArrayList<IN>> ht;
	private ArrayList<IN> probeBuffered;
	private boolean buildDone;
	private boolean probeDone;

	private boolean reuse = false;

	@Override
	public void openOutBag() {
		super.openOutBag();
		probeBuffered = new ArrayList<>();
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
				ht = new HashMap<>(8192);
			}
		}
	}

	@Override
	public void pushInElement(IN e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		if (logicalInputId == 0) { // build side
			assert !buildDone;
			K key = keyExtr(e);
			ArrayList<IN> l = ht.get(key);
			if (l == null) {
				l = new ArrayList<>();
				l.add(e);
				ht.put(key,l);
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
			//LOG.info("Build side finished");
			buildDone = true;
			for (IN e: probeBuffered) {
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

	private void probe(IN e) {
		ArrayList<IN> l = ht.get(keyExtr(e));
		if (l != null) {
			for (IN b: l) {
				out.collectElement(Tuple2.of(b, e));
			}
		}
	}

	protected abstract K keyExtr(IN e);
}
