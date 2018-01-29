package gg;

import gg.util.Unit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: We couldn't directly tell the CFLManager from the driver, because it is important
// that these calls to the CFLManager happen on the TMs.
public class KickoffSource implements SourceFunction<Unit> {

	private static final Logger LOG = LoggerFactory.getLogger(KickoffSource.class);

	private final int[] kickoffBBs;
	private int terminalBBId = -2;
	private CFLConfig cflConfig;

	public KickoffSource(int... kickoffBBs) {
		this.kickoffBBs = kickoffBBs;
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		this.cflConfig = CFLConfig.getInstance();
		assert this.terminalBBId >= 0 : "CFLConfig has to be set before creating KickoffSource";
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		LOG.info("KickoffSource kicking off");
		CFLManager cflManager = CFLManager.getSing();

		//cflManager.resetCFL(); // Ezt atmozgattam a TaskManager.scala-ba
		cflManager.specifyTerminalBB(terminalBBId);

		assert cflConfig.numToSubscribe != -10;
		cflManager.specifyNumToSubscribe(cflConfig.numToSubscribe);

		for(int bb: kickoffBBs) {
			cflManager.appendToCFL(bb);
		}
	}

	@Override
	public void cancel() {

	}
}
