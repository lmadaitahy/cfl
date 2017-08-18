package gg.operators;

import gg.util.SerializedBuffer;
import gg.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

/**
 * Each time when the control flow reaches the operator, we create a new out file.
 * First input is a singleton bag with an Integer, which is appended to the filename.
 * Second input is a bag of Integers, which are written to the current file.
 *
 * Warning: At the moment, it only works with para 1.
 */
public class CFAwareFileSink extends BagOperator<Integer, Unit> implements DontThrowAwayInputBufs {

    private final String baseName;

    private String currentFileName;

    private SerializedBuffer<Integer> buffer;

    private static final TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());

    private boolean[] closed = new boolean[2];

    public CFAwareFileSink(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();

        assert host.subpartitionId == 0; // we need para 1

        buffer = new SerializedBuffer<>(integerSer);

        closed[0] = false;
        closed[1] = false;

        currentFileName = null;
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        if (logicalInputId == 0) {
            currentFileName = baseName + e;
        } else {
            assert logicalInputId == 1;
            buffer.add(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        closed[inputId] = true;

        if (closed[0] && closed[1]) {
            try {
                PrintWriter writer = new PrintWriter(currentFileName, "UTF-8");
                for (Integer e : buffer) {
                    writer.println(Integer.toString(e));
                }
                buffer = null;
                writer.close();
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            out.closeBag();
        }
    }
}
