package eu.stratosphere.labyrinth.partitioners;

public class Forward<T> extends Partitioner<T> {

	public Forward(int targetPara) {
		super(targetPara);
	}

	@Override
	public short getPart(T e, short subpartitionId) {
		return subpartitionId;
	}
}
