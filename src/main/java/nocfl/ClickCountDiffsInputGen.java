package nocfl;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Random;

public class ClickCountDiffsInputGen {

    public static void main(String[] args) throws Exception {
        Random rnd = new Random();

        final String pref = args[0] + "/";
        final String pageAttributesFile = pref + "in/pageAttributes.tsv";

        final int numProducts = 100000000;
        final int numDays = 365;
        final double clicksPerDayRatio = 1.0 / 100;
        final int numClicksPerDay = (int)(numProducts * clicksPerDayRatio);

        new File(pref + "in").mkdirs();
        new File(pref + "out").mkdirs();
        new File(pref + "tmp").mkdirs();

        Writer wr1 = new FileWriter(pageAttributesFile);
        for (int i=0; i<numProducts; i++) {
            int type = rnd.nextInt(2);
            wr1.write(Integer.toString(i) + "\t" + Integer.toString(type) + "\n");
        }
        wr1.close();

        for (int day = 1; day <= numDays; day++) {
            System.out.println(day);
            Writer wr2 = new FileWriter(pref + "in/clickLog_" + day);
            for (int i=0; i<numClicksPerDay; i++) {
                int click = rnd.nextInt(numProducts);
                wr2.write(Integer.toString(click) + "\n");
            }
            wr2.close();
        }
    }

}
