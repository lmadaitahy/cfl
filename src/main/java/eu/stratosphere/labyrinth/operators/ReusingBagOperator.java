package eu.stratosphere.labyrinth.operators;

/**
 * For reusing input bags across multiple output bags.
 */
public interface ReusingBagOperator {
    void signalReuse();
}
