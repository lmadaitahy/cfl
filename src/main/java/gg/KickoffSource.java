package gg;

import gg.util.Unit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

// Note: We couldn't directly tell the CFLManager from the driver, because it is important
// that these calls to the CFLManager happen a on the TMs.
public class KickoffSource implements SourceFunction<Unit> {

	private final int[] kickoffBBs;
	private int terminalBBId = -2;

	public KickoffSource(int... kickoffBBs) {
		this.kickoffBBs = kickoffBBs;
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		assert this.terminalBBId >= 0;
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		CFLManager cflManager = CFLManager.getSing();

		cflManager.resetCFL();
		cflManager.specifyTerminalBB(terminalBBId);

		for(int bb: kickoffBBs) {
			cflManager.appendToCFL(bb);
		}
	}

	@Override
	public void cancel() {

	}
}
