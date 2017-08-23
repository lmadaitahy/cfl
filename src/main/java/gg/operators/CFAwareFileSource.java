package gg.operators;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Can have >1 para. Set partitioning to random.
// However, it reads a single file non-parallel.
public abstract class CFAwareFileSource<T> extends BagOperator<Integer, T> {

    private final String baseName;

    private ExecutorService es;

    private boolean hadInput;


    public CFAwareFileSource(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public void setup() {
        super.setup();
        es = Executors.newSingleThreadExecutor();
    }

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        hadInput = false;
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        hadInput = true;

        es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String path = baseName + e;
                    FileSystem fs = FileSystem.get(new URI(path));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))), 131072);

                    String line;
                    line = br.readLine();
                    while (line != null) {
                        out.collectElement(parseLine(line));
                        line = br.readLine();
                    }
                    out.closeBag();
                } catch (URISyntaxException | IOException e2) {
                    throw new RuntimeException(e2);
                }
            }
        });
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert inputId == 0;

        if (!hadInput) {
            out.closeBag();
        }
    }

    abstract protected T parseLine(String line);
}
