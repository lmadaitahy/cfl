package gg.operators;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

// Don't forget to set the parallelism to 1!
public abstract class CFAwareFileSource<T> extends SingletonBagOperator<Integer, T> {

    final String baseName;
    final boolean hdfs;

    public CFAwareFileSource(String baseName) {
        this.baseName = baseName;
        this.hdfs = baseName.startsWith("hdfs");
    }

    @Override
    public void pushInElement(Integer e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        if (hdfs) {
            readFromHDFS(e);
        } else {
            readNormalFile(e);
        }
    }

    private void readNormalFile(int e) {
        String fileName = baseName + e;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                out.collectElement(parseLine(line));
            }
        } catch (FileNotFoundException e1) {
            throw new RuntimeException("File not found: " + fileName);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    private void readFromHDFS(int e) {
        try {
            Path pt = new Path(baseName + e);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null){
                out.collectElement(parseLine(line));
                line = br.readLine();
            }
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    abstract protected T parseLine(String line);
}
