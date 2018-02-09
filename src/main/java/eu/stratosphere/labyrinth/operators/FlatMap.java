package eu.stratosphere.labyrinth.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public abstract class FlatMap<IN,OUT> extends BagOperator<IN,OUT> {

	@Override
	public void openOutBag() {
		super.openOutBag();
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		out.closeBag();
	}

	public static <IN, OUT> FlatMap<IN, OUT> create(FlatMapFunction<IN, OUT> f) {
		return new FlatMap<IN, OUT>() {
			@Override
			public void pushInElement(IN e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				try {
					f.flatMap(e, new Collector<OUT>() {
						@Override
						public void collect(OUT x) {
							out.collectElement(x);
						}

						@Override
						public void close() {
							out.closeBag();
						}
					});
				} catch (Exception e1) {
					assert false;
				}
			}
		};
	}
}
