package gg.partitioners;

public class RoundRobin<T> extends Partitioner<T> {

	private short i = 0;

	public RoundRobin(int targetPara) {
		super((short)targetPara);
	}

	@Override
	public short getPart(T elem) {
		short ret = i;
		i++;
		if (i >= targetPara){
			i = 0;
		}
		return ret;
	}
}
