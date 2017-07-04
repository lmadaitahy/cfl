package gg.partitioners;

public class Always0<T> extends Partitioner<T> {

	public Always0(int targetPara) {
		super(targetPara);
	}

	@Override
	public short getPart(T e, short subpartitionId) {
		return 0;
	}
}
