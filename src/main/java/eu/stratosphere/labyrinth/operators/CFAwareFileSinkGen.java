package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.SerializedBuffer;
import eu.stratosphere.labyrinth.util.Unit;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Either;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Each time when the control flow reaches the operator, we create a new out file.
 * First input is a singleton bag with an Integer, which is appended to the filename.
 * Second input is a bag of Integers, which are written to the current file.
 *
 * Warning: At the moment, it only works with para 1.
 */
public class CFAwareFileSinkGen<T> extends BagOperator<Either<Integer, T>, Unit> implements DontThrowAwayInputBufs {

    private final String baseName;

    private String currentFileName;

    private SerializedBuffer<T> buffer;

    private final TypeSerializer<T> inputSer;

    private boolean[] closed = new boolean[2];

    public CFAwareFileSinkGen(String baseName, TypeSerializer<T> inputSer) {
        this.baseName = baseName;
        this.inputSer = inputSer;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();

        assert host.subpartitionId == 0; // we need para 1

        buffer = new SerializedBuffer<>(inputSer);

        closed[0] = false;
        closed[1] = false;

        currentFileName = null;
    }

    @Override
    public void pushInElement(Either<Integer, T> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        if (logicalInputId == 0) {
            assert e.isLeft();
            currentFileName = baseName + e.left();
        } else {
            assert logicalInputId == 1;
            assert e.isRight();
            buffer.add(e.right());
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        closed[inputId] = true;

        if (closed[0] && closed[1]) {
            try {

                FileSystem fs = FileSystem.get(new URI(currentFileName));
                //BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(currentFileName))));

                //PrintWriter writer = new PrintWriter(currentFileName, "UTF-8");
                PrintWriter writer = new PrintWriter(fs.create(new Path(currentFileName), FileSystem.WriteMode.OVERWRITE));
                for (T e : buffer) {
                    writer.println(e.toString());
                }
                buffer = null;
                writer.close();
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }

            out.closeBag();
        }
    }
}
