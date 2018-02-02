package eu.stratosphere.labyrinth.operators;

import eu.stratosphere.labyrinth.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class FileSink<T> extends BagOperator<T, Unit> {

    private static final Logger LOG = LoggerFactory.getLogger(Print.class);

    private final String path;
    protected PrintWriter writer;

    public FileSink(String path) {
        this.path = path;

        // Create the directory here in the ctor, to not try it para times.
        File f = new File(path);
        if (f.exists()) {
            throw new RuntimeException("Output file already exists (" + path + ")");
        }
        boolean r = f.mkdirs();
        assert r;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();

        try {
            writer = new PrintWriter(path + "/" + host.subpartitionId, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected void print(T e) {
        writer.println(e);
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        print(e);
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        writer.close();

        out.closeBag();
    }
}
