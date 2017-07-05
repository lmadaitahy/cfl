package gg.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class SerializedBuffer<T> implements Iterable<T> {

    private static final int segSize = 32768;

    private final TypeSerializer<T> ser;

    private final ArrayList<MemorySegment> segs = new ArrayList<>();

    private final SimpleCollectingOutputView outView = new SimpleCollectingOutputView(segs, new ConjuringSegmentSource(), segSize);

    private boolean consumeStarted = false;

    private int numWritten = 0;
    private int numRead = 0;

    public SerializedBuffer(TypeSerializer<T> ser) {
        this.ser = ser;
    }


    private ArrayList<T> xx = new ArrayList<T>();


    public void insert(T e) {
        assert !consumeStarted;  //todo: ez miert nem igaz?
        xx.add(e);
        numWritten++;
        try {
            ser.serialize(e, outView);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    public int size() {
        return numWritten;
    }

    @Override
    public Iterator<T> iterator() {
        consumeStarted = true;
        return new ElementIterator();
    }

    public final class ElementIterator implements Iterator<T> {

        RandomAccessInputView inView = new RandomAccessInputView(segs, segSize);

        @Override
        public boolean hasNext() {
            assert numRead <= numWritten;
            return numRead < numWritten;
        }

        int ind = 0;

        @Override
        public T next() {
            assert numWritten == xx.size();
            numRead++;
            assert numRead <= numWritten;
            try {
                T ret = ser.deserialize(inView);

                assert xx.get(ind).equals(ret);
                ind++;

                return ret;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private static final class ConjuringSegmentSource implements MemorySegmentSource {
        @Override
        public MemorySegment nextSegment() {
            return HeapMemorySegment.FACTORY.allocateUnpooledSegment(segSize, null);
        }
    }
}
