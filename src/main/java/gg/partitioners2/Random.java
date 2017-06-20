package gg.partitioners2;

public class Random<T> extends Partitioner<T> {

	public Random(int targetPara) {
		super(targetPara);
	}

	private final java.util.Random rnd = new java.util.Random();

	@Override
	public short getPart(T e) {
		return (short)rnd.nextInt(targetPara);
	}
}
