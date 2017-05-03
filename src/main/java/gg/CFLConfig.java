package gg;

public class CFLConfig {
    private static CFLConfig sing = new CFLConfig();

    public static CFLConfig getInstance() {
        return sing;
    }

    private CFLConfig() {}


    public int terminalBBId = -1;
}
