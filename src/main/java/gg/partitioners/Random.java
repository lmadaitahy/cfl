package gg.partitioners;

public class Random<T> extends Partitioner<T> {

	public Random(int targetPara) {
		super(targetPara);
	}

	private final java.util.Random rnd = new java.util.Random();

	@Override
	public short getPart(T e, short subpartitionId) {
		return (short)rnd.nextInt(targetPara);
	}
}
