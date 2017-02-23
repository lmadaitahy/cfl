package gg;

import gg.util.Unit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class KickoffSource implements SourceFunction<Unit> {

	int[] kickoffBBs;

	public KickoffSource(int... kickoffBBs) {
		this.kickoffBBs = kickoffBBs;
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		CFLManager cflManager = CFLManager.getSing();

		cflManager.resetCFL();

		for(int bb: kickoffBBs) {
			cflManager.appendToCFL(bb);
		}
	}

	@Override
	public void cancel() {

	}
}
