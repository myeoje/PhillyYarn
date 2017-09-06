package org.apache.hadoop.yarn.sls;

/**
 * Created by wencong on 17-5-26.
 */
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class TestSLSSimpleRunner {

    @Test
    @SuppressWarnings("all")
    public void testSimulatorRunning() throws Exception {
        File tempDir = new File("target", UUID.randomUUID().toString());

        // start the simulator
        File slsOutputDir = new File(tempDir.getAbsolutePath() + "/slsoutput/");
        String args[] = new String[]{
                //"-inputrumen", "src/main/data/2jobs2min-rumen-jh.json",
                "-inputsls", "src/main/data/philly-sls-jobs.json",
                "-nodes", "src/main/data/less-sls-nodes.json",
                "-output", "src/main/data/"};
        SLSSimpleRunner.main(args);
    }

}
