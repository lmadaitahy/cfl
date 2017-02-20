package gg.partitioners;

public class RoundRobin<T> extends EventBroadcast<T> {

	private int i = 0;

	@Override
	protected int selectForElement(T elem, int numberOfOutputChannels) {
		int ret = i;
		i++;
		if(i>=numberOfOutputChannels){
			i=0;
		}
		return ret;
	}
}
