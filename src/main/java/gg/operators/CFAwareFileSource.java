package gg.operators;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

// Can have >1 para. Set partitioning to RoundRobin.
// However, it reads a single file non-parallel.
public abstract class CFAwareFileSource<T> extends BagOperator<Integer, T> {

    private static final Logger LOG = LoggerFactory.getLogger(CFAwareFileSource.class);

    private final String baseName;

    public CFAwareFileSource(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        try {
            String path = baseName + e;

            LOG.info("Reading file " + path);

            FileSystem fs = FileSystem.get(new URI(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))), 131072);

            String line;
            line = br.readLine();
            while (line != null) {
                out.collectElement(parseLine(line));
                line = br.readLine();
            }
        } catch (URISyntaxException | IOException e2) {
            throw new RuntimeException(e2);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert inputId == 0;
        out.closeBag();
    }

    abstract protected T parseLine(String line);
}
