package gg;

import gg.util.Unit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: We couldn't directly tell the CFLManager from the driver, because it is important
// that these calls to the CFLManager happen on the TMs.
public class KickoffSource implements SourceFunction<Unit> {

	private static final Logger LOG = LoggerFactory.getLogger(KickoffSource.class);

	private final int[] kickoffBBs;
	private int terminalBBId = -2;

	public KickoffSource(int... kickoffBBs) {
		this.kickoffBBs = kickoffBBs;
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		assert this.terminalBBId >= 0;
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		LOG.info("KickoffSource kicking off");
		CFLManager cflManager = CFLManager.getSing();

		//cflManager.resetCFL(); // Ezt atmozgattam a TaskManager.scala-ba
		cflManager.specifyTerminalBB(terminalBBId);

		for(int bb: kickoffBBs) {
			cflManager.appendToCFL(bb);
		}
	}

	@Override
	public void cancel() {

	}
}
