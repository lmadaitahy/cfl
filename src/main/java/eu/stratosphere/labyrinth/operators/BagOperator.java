package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.BagOperatorHost;
import eu.stratosphere.labyrinth.BagOperatorOutputCollector;
import eu.stratosphere.labyrinth.CFLConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class BagOperator<IN, OUT> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperator.class);

	protected BagOperatorOutputCollector<OUT> out;

	protected TypeSerializer<IN> inSer;

	private boolean[] open = new boolean[]{false, false, false};

	protected String name;

	protected BagOperatorHost<IN, OUT> host;

	public void giveHost(BagOperatorHost<IN, OUT> host) {
		this.host = host;
	}

	public void openInBag(int logicalInputId) {
		if (CFLConfig.vlog) LOG.info("openInBag[" + name + "]: logicalInputId: " + logicalInputId);
		assert !open[logicalInputId];
		open[logicalInputId] = true;
	}

	public final void setName(String name) {
		this.name = name;
	}

	public final void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		this.out = out;
	}

	public final void giveInputSerializer(TypeSerializer<IN> serializer) { this.inSer = serializer; }

	public void openOutBag() {
		if (CFLConfig.vlog) LOG.info("openOutBag[" + name + "]");
	}

	public void pushInElement(IN e, int logicalInputId) {
		if (CFLConfig.vlog) LOG.info("pushInElement[" + name + "]: e: " + e + " logicalInputId: " + logicalInputId);
		assert open[logicalInputId];
	}

	// Warning: Overriding methods should pay attention to closing the out bag.
	public void closeInBag(int inputId) {
		if (CFLConfig.vlog) LOG.info("closeInBag[" + name + "]: inputId: " + inputId);
		assert open[inputId];
		open[inputId] = false;
	}

}
