package eu.stratosphere.labyrinth;

public interface BagOperatorOutputCollector<T> {

	void collectElement(T e);

	/**
	 * closes our partition of the bag
	 *
	 * WARNING:
	 *   Do not alter your internal state after calling closeBag, since the runtime might call openOutBag for your next
	 *   bag before closeBag returns.
	 *   For example, do not do any cleanup in your closeInBag method after calling closeBag because you might clean up
	 *   the newly created state for your next bag instead. In other words, calling closeBag should be the very last
	 *   thing you do in your closeInBag method.
 	 */
	void closeBag();

	void appendToCfl(int bbId);
}
