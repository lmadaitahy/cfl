package gg.operators;

public class ClickLogReader extends CFAwareFileSource<Integer> {

    public ClickLogReader(String baseName) {
        super(baseName);
    }

    @Override
    protected Integer parseLine(String line) {
        return Integer.parseInt(line);
    }
}
