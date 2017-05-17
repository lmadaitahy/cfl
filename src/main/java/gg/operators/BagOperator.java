package gg.operators;

import gg.BagOperatorOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class BagOperator<IN, OUT> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperator.class);

	protected BagOperatorOutputCollector<OUT> out;

	private boolean[] open = new boolean[]{false, false};

	private String name;

	public final void openInBag(int logicalInputId) {
		assert !open[logicalInputId];
		open[logicalInputId] = true;
	}

	public final void setName(String name) {
		this.name = name;
	}

	public final void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		this.out = out;
	}


	public void openOutBag() {
		LOG.info("openOutBag[" + name + "]");
	}

	public void pushInElement(IN e, int logicalInputId) {
		assert open[logicalInputId];
		LOG.info("pushInElement[" + name + "]: e: " + e + " logicalInputId: " + logicalInputId);
	}

	public void closeInBag(int inputId) {
		assert open[inputId];
		open[inputId] = false;
		LOG.info("closeInBag[" + name + "]: inputId: " + inputId);
	}

}
