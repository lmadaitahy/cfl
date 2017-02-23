package gg.partitioners;

public class Random<T> extends EventBroadcast<T> {

	private final java.util.Random rnd = new java.util.Random();

	@Override
	protected int selectForElement(T elem, int numberOfOutputChannels) {
		return rnd.nextInt(numberOfOutputChannels);
	}
}
