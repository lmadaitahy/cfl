package gg.operators;

import gg.BagOperatorOutputCollector;

import java.io.Serializable;

public class Identity<T> implements BagOperator<T,T>, Serializable {

    private BagOperatorOutputCollector<T> out;

    @Override
    public void giveOutputCollector(BagOperatorOutputCollector<T> out) {
        this.out = out;
    }

    @Override
    public void OpenInBag() {}

    @Override
    public void pushInElement(T e) {
        out.collectElement(e);
    }

    @Override
    public void closeInBag(int inputId) {
        out.closeBag();
    }
}
