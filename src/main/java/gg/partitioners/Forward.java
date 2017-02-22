package gg.partitioners;

public class Forward<T> extends EventBroadcast<T> {

	@Override
	protected int selectForElement(T elem, int numberOfOutputChannels) {
		return 0;
	}
}
