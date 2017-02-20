package gg;

public interface BagOperator<IN, OUT> {

	void giveOutputCollector(BagOperatorOutputCollector<OUT> out);

	void OpenInBag();

	void pushInElement(IN e);

	void closeInBag();

}
