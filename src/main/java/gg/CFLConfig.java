package gg;

public class CFLConfig {
    private static CFLConfig sing = new CFLConfig();

    public static CFLConfig getInstance() {
        return sing;
    }

    private CFLConfig() {}


    // Ezt be kell allitani meg a KickoffSource letrehozasa elott, mert az elrakja a ctorban.
    // Tovabba a job elindulasakor mar ennek be kell lennie allitva (vagyis nem lehetne a KickoffSource setupjaban bealltani ezt itt),
    // mert a BagOperatorHost setupjaban szukseg van ra, es a setupok sorrendje nem determinisztikus.
    public int terminalBBId = -1;
}
