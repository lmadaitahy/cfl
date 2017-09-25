package gg;

import gg.jobs.ClickCountDiffs;
import gg.jobsold.*;
import inputgen.ClickCountDiffsInputGen;
import org.apache.commons.io.FileUtils;
import org.apache.flink.runtime.client.JobCancellationException;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class CFLITCase {

    @Test(expected=JobCancellationException.class)
    public void testNoCF() throws Exception {
        NoCF.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testNoCFNew() throws Exception {
        LabyNode.labyNodes.clear();
        gg.jobs.NoCF.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testEmptyBags() throws Exception {
        EmptyBags.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCF() throws Exception {
        SimpleCF.main(new String[]{"100"});
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFNew() throws Exception {
        LabyNode.labyNodes.clear();
        gg.jobs.SimpleCF.main(new String[]{"100"});
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFDataSize() throws Exception {
        SimpleCFDataSize.main(new String[]{"50", "500"});
    }

    @Test(expected=JobCancellationException.class)
    public void testConnectedComponents() throws Exception {
        ConnectedComponents.main(new String[]{});
    }

    @Test(expected=JobCancellationException.class)
    public void testConnectedComponentsMB() throws Exception {
        ConnectedComponentsMB.main(new String[]{});
    }

    @Test()
    public void testClickCountDiffs() throws Exception {
        LabyNode.labyNodes.clear();

        String path = "/tmp/ClickCountITCase/";
        FileUtils.deleteQuietly(new File(path));

        int size = 100000;
        int numDays = 30;

        path = ClickCountDiffsInputGen.generate(size, numDays, path, new Random(1234), 0.01);

        boolean exceptionReceived = false;
        try {
            ClickCountDiffs.main(new String[]{path, Integer.toString(numDays)});
        } catch (JobCancellationException ex) {
            exceptionReceived = true;
        }
        if (!exceptionReceived) {
            throw new RuntimeException("testClickCountDiffs job failed");
        }

        int[] exp = new int[]{1010, 1032, 981, 977, 978, 981, 988, 987, 958, 997, 985, 994, 1001, 987, 1007, 971, 960, 976, 1025, 1022, 971, 993, 997, 996, 1038, 985, 974, 999, 1020};
        ClickCountDiffsInputGen.checkLabyOut(path, numDays, exp);

        int nocflNumDays = numDays/6;
        nocfl.ClickCountDiffs.main(new String[]{"/tmp/ClickCountITCase/" + size, Integer.toString(nocflNumDays)});
        ClickCountDiffsInputGen.checkNocflOut(path, nocflNumDays, exp);
    }

//    @Test()
//    public void debug() throws Exception {
//        for(int i=0; i<100; i++) {
//            LabyNode.labyNodes.clear();
//            try {
//                //gg.jobs.NoCF.main(null);
//                ConnectedComponentsMB.main(new String[]{});
//            } catch (JobCancellationException ex) {
//                //ok
//            }
//        }
//    }
}
