package eu.stratosphere.labyrinth;

public interface BagOperatorOutputCollector<T> {

	void collectElement(T e);

	void closeBag(); // closes our partition of the bag

	void appendToCfl(int bbId);
}
