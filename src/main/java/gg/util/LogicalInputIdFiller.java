package gg.util;

import gg.ElementOrEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class LogicalInputIdFiller<T> implements MapFunction<ElementOrEvent<T>,ElementOrEvent<T>> {

    private final byte logicalInputId;

    public LogicalInputIdFiller(int logicalInputId) {
        this.logicalInputId = (byte)logicalInputId;
    }

    @Override
    public ElementOrEvent<T> map(ElementOrEvent<T> e) throws Exception {
        ElementOrEvent<T> r = e.copy();
        r.logicalInputId = logicalInputId;
        return r;
    }
}
