package org.apache.hadoop.yarn.sls.scheduler;

import org.apache.hadoop.yarn.sls.appmaster.AMSimpleSimulator;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimpleSimulator;
import org.apache.log4j.Logger;
import sun.java2d.pipe.SpanShapeRenderer;

import java.util.ArrayList;

/**
 * Created by wencong on 17-5-26.
 */
public class SimpleTimer {

    // we only support second level timer


    // in second level
    public static int currentTime;

    // 3 month
    private final int max_time_in_second = 3 * 24 * 60 * 60;

    private ArrayList<ArrayList<NMSimpleSimulator>> nmTimeline;
    private ArrayList<ArrayList<AMSimpleSimulator>> amTimeline;

    private int finishedAM;
    private int totalAM;
    protected final Logger LOG = Logger.getLogger(SimpleTimer.class);
    public SimpleTimer()
    {
        currentTime = 0;
        finishedAM = 0;

        nmTimeline = new ArrayList<ArrayList<NMSimpleSimulator>>();
        amTimeline = new ArrayList<ArrayList<AMSimpleSimulator>>();
        for (int i =0;i<max_time_in_second;i++)
        {
            nmTimeline.add(new ArrayList<NMSimpleSimulator>());
            amTimeline.add(new ArrayList<AMSimpleSimulator>());
        }
    }

    public void addFinishedAM()
    {
        finishedAM ++;
    }

    public void setTotalAM(int totAM)
    {
        totalAM = totAM;
    }



    public void scheduleNM(NMSimpleSimulator nm)
    {
        int heartbeatInSecond = nm.getHeartBeatIntervalSecond();
        if (currentTime + heartbeatInSecond < max_time_in_second) {
            nmTimeline.get(currentTime + heartbeatInSecond).add(nm);
        }
    }

    public void scheduleAM(AMSimpleSimulator am)
    {
        int traceSubmitTimeSecond = am.getTraceSubmitTimeSecond();

        if (currentTime >= traceSubmitTimeSecond) {
            // if job already submitted
            int heartbeatInSecond = am.getHeartBeatIntervalSecond();
            if (currentTime + heartbeatInSecond < max_time_in_second) {
                amTimeline.get(currentTime + heartbeatInSecond).add(am);
            }
        }
        else {
            // if not submitted
            amTimeline.get(currentTime + traceSubmitTimeSecond).add(am);
        }
    }

    public void run() {
        for (int t = 1; t < max_time_in_second; t++) {

            long start = System.currentTimeMillis();

            currentTime = t;
            // NM item
            ArrayList<NMSimpleSimulator> nmList = nmTimeline.get(currentTime);
            for (int i = 0; i < nmList.size(); i++)
            {
                NMSimpleSimulator nm = nmList.get(i);
                nm.run();
                scheduleNM(nm);
            }

            // AM item
            ArrayList<AMSimpleSimulator> amList = amTimeline.get(currentTime);
            for (int i =0;i<amList.size();i++)
            {
                AMSimpleSimulator am = amList.get(i);
                am.run();
                if (!am.amIDoneWithLastStep()) scheduleAM(am);
            }

            if (finishedAM == totalAM)
            {
                break;
            }
            long end = System.currentTimeMillis();
            long process_time = end - start;
            if (process_time > 1000) {
                LOG.info("process NM and AM takes more than 1 second !!!");
            }
            else
            {
                try {
                    Thread.sleep(1000 - process_time);

                }
                catch (Exception e)
                {
                    LOG.error("thread sleep exception");
                }
            }
        }
    }
}
