/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.appmaster.AMSimpleSimulator;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimpleSimulator;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.SimpleTimer;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import sun.java2d.pipe.SpanShapeRenderer;


@Private
@Unstable
public class SLSSimpleRunner {



    // RM, Runner
    private ResourceManager rm;
    private YarnClient yarnClient;
    private FairScheduler fs;
    //private static TaskRunner runner = new TaskRunner();
    private static SimpleTimer simpleTimer = new SimpleTimer();
    private String[] inputTraces;
    private Configuration conf;
    private Map<String, Integer> queueAppNumMap;

    // NM simulator
    private HashMap<NodeId, NMSimpleSimulator> nmMap;
    private int nmMemoryMB, nmVCores;
    private String nodeFile;

    // AM simulator
    private int AM_ID;
    private Map<String, AMSimpleSimulator> amMap;
    private Set<String> trackedApps;
    private Map<String, Class> amClassMap;
    private static int remainingApps = 0;

    // metrics
    private String metricsOutputDir;
    private boolean printSimulation;

    // other simulation information
    private int numNMs, numRacks, numAMs, numTasks;
    private long maxRuntime;
    public final static Map<String, Object> simulateInfoMap =
            new HashMap<String, Object>();

    // logger
    public final static Logger LOG = Logger.getLogger(SLSSimpleRunner.class);

    // input traces, input-rumen or input-sls
    private boolean isSLS;

    private long smallestJobSubmitTime;

    private static long processStartTime;
    private static final long timeScale = 3 * 24;

    public static long NOW(){
        // should be system current time to pass the test case
        return System.currentTimeMillis();

        //return processStartTime + (System.currentTimeMillis() - processStartTime) * timeScale;
    }


    public SLSSimpleRunner(boolean isSLS, String inputTraces[], String nodeFile,
                           String outputDir, Set<String> trackedApps,
                           boolean printsimulation)
            throws IOException, ClassNotFoundException {
        this.isSLS = isSLS;
        this.inputTraces = inputTraces.clone();
        this.nodeFile = nodeFile;
        this.trackedApps = trackedApps;
        this.printSimulation = printsimulation;
        metricsOutputDir = outputDir;

        nmMap = new HashMap<NodeId, NMSimpleSimulator>();
        queueAppNumMap = new HashMap<String, Integer>();
        amMap = new HashMap<String, AMSimpleSimulator>();
        amClassMap = new HashMap<String, Class>();

        // runner configuration
        conf = new Configuration(false);
        conf.addResource("sls-runner.xml");
        // runner
        int poolSize = conf.getInt(SLSConfiguration.RUNNER_POOL_SIZE,
                SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
        //SLSRunner.runner.setQueueSize(poolSize);
        // <AMType, Class> map
        for (Map.Entry e : conf) {
            String key = e.getKey().toString();
            if (key.startsWith(SLSConfiguration.AM_TYPE)) {
                String amType = key.substring(SLSConfiguration.AM_TYPE.length());
                amClassMap.put(amType, Class.forName(conf.get(key)));
            }
        }

        processStartTime = System.currentTimeMillis();
        smallestJobSubmitTime = Long.MAX_VALUE;
    }

    public void start() throws Exception {
        // start resource manager
        startRM();
        // start node managers
        startNM();
        // start application masters
        startAM();
        // set queue & tracked apps information
        ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                .setQueueSet(this.queueAppNumMap.keySet());
        ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                .setTrackedAppSet(this.trackedApps);
        // print out simulation info
        printSimulationInfo();
        // blocked until all nodes RUNNING
        waitForNodesRunning();
        // starting the runner once everything is ready to go,
        //runner.start();
        simpleTimer.run();
    }

    private void startRM() throws IOException, ClassNotFoundException {
        Configuration rmConf = new YarnConfiguration();
        String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);
        rmConf.set(SLSConfiguration.RM_SCHEDULER, schedulerClass);
        rmConf.set(YarnConfiguration.RM_SCHEDULER,
                ResourceSchedulerWrapper.class.getName());
        rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);
        rm = new ResourceManager();
        rm.init(rmConf);
        rm.start();

        // Wencong: add YarnClient for getNodeReports
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(rmConf);
        yarnClient.start();
    }

    private void startNM() throws YarnException, IOException {
        // nm configuration
        nmMemoryMB = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
                SLSConfiguration.NM_MEMORY_MB_DEFAULT);
        nmVCores = conf.getInt(SLSConfiguration.NM_VCORES,
                SLSConfiguration.NM_VCORES_DEFAULT);
        int heartbeatInterval = conf.getInt(
                SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
                SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
        // nm information (fetch from topology file, or from sls/rumen json file)
        Set<String> nodeSet = new HashSet<String>();
        if (nodeFile.isEmpty()) {
            if (isSLS) {
                for (String inputTrace : inputTraces) {
                    nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
                }
            } else {
                for (String inputTrace : inputTraces) {
                    nodeSet.addAll(SLSUtils.parseNodesFromRumenTrace(inputTrace));
                }
            }

        } else {
            nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile));
        }
        // create NM simulators
        Random random = new Random();
        Set<String> rackSet = new HashSet<String>();
        for (String hostName : nodeSet) {
            // we randomize the heartbeat start time from zero to 1 interval

            NMSimpleSimulator nm = new NMSimpleSimulator();
            nm.init(hostName, nmMemoryMB, nmVCores,
                    random.nextInt(heartbeatInterval), heartbeatInterval, rm, fs);
            simpleTimer.scheduleNM(nm);
            nmMap.put(nm.getNode().getNodeID(), nm);
            //runner.schedule(nm);
            rackSet.add(nm.getNode().getRackName());
        }
        numRacks = rackSet.size();
        numNMs = nmMap.size();
        LOG.debug(MessageFormat.format("#Rack: {0}; #Node: {1}", numRacks, numNMs));
    }

    private void waitForNodesRunning() throws InterruptedException {
        long startTimeMS = SLSSimpleRunner.NOW();
        while (true) {
            int numRunningNodes = 0;
            for (RMNode node : rm.getRMContext().getRMNodes().values()) {
                if (node.getState() == NodeState.RUNNING) {
                    numRunningNodes ++;
                }
            }
            if (numRunningNodes == numNMs) {
                break;
            }
            LOG.info(MessageFormat.format("SLSSimpleRunner is waiting for all " +
                            "nodes RUNNING. {0} of {1} NMs initialized.",
                    numRunningNodes, numNMs));
            Thread.sleep(1000);
        }
        LOG.info(MessageFormat.format("SLSSimpleRunner takes {0} ms to launch all nodes.",
                (SLSSimpleRunner.NOW() - startTimeMS)));
        long now = SLSSimpleRunner.NOW();
        if (smallestJobSubmitTime < now) {
            LOG.warn(MessageFormat.format("job submit before all nodes ready: {0} < {1}", smallestJobSubmitTime, now));
        }
    }

    @SuppressWarnings("unchecked")
    private void startAM() throws YarnException, IOException {
        // application/container configuration
        int heartbeatInterval = conf.getInt(
                SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
                SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
        int containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
                SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
        int containerVCores = conf.getInt(SLSConfiguration.CONTAINER_VCORES,
                SLSConfiguration.CONTAINER_VCORES_DEFAULT);
        Resource containerResource =
                BuilderUtils.newResource(containerMemoryMB, containerVCores);

        // application workload
        if (isSLS) {
            startAMFromSLSTraces(containerResource, heartbeatInterval);
        } else {
            startAMFromRumenTraces(containerResource, heartbeatInterval);
        }
        numAMs = amMap.size();
        remainingApps = numAMs;
    }

    /**
     * parse workload information from sls trace files
     */
    @SuppressWarnings("unchecked")
    private void startAMFromSLSTraces(Resource containerResource,
                                      int heartbeatInterval) throws IOException {


        int totalAMCount = 0;
        // parse from sls traces
        JsonFactory jsonF = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper();
        for (String inputTrace : inputTraces) {
            Reader input = new FileReader(inputTrace);
            try {
                Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
                        Map.class);
                while (i.hasNext()) {
                    Map jsonJob = i.next();

                    // load job information
                    long jobSubmitTime = Long.parseLong(
                            jsonJob.get("job.submit.ms").toString());
                    long jobStartTime = Long.parseLong(
                            jsonJob.get("job.start.ms").toString());
                    long jobFinishTime = Long.parseLong(
                            jsonJob.get("job.end.ms").toString());

                    String user = (String) jsonJob.get("job.user");
                    if (user == null)  user = "default";
                    String queue = jsonJob.get("job.queue.name").toString();

                    String oldAppId = jsonJob.get("job.id").toString();
                    boolean isTracked = trackedApps.contains(oldAppId);
                    int queueSize = queueAppNumMap.containsKey(queue) ?
                            queueAppNumMap.get(queue) : 0;
                    queueSize ++;
                    queueAppNumMap.put(queue, queueSize);
                    String amType = jsonJob.get("am.type").toString();
                    List<ContainerSimulator> containerList =
                            new ArrayList<ContainerSimulator>();
                    int jobGpu = 0;

                    // Wencong: TODO: hack for debugging
                    if (amType.equals("philly"))
                    {
                        // wencong: hack for philly job input
                        jobGpu = Integer.parseInt(
                                jsonJob.get("job.gpu").toString());
                    }
                    else {
                        // tasks
                        List tasks = (List) jsonJob.get("job.tasks");
                        if (tasks == null || tasks.size() == 0) {
                            continue;
                        }
                        for (Object o : tasks) {
                            Map jsonTask = (Map) o;
                            String hostname = jsonTask.get("container.host").toString();
                            long taskStart = Long.parseLong(
                                    jsonTask.get("container.start.ms").toString());
                            long taskFinish = Long.parseLong(
                                    jsonTask.get("container.end.ms").toString());
                            long lifeTime = taskFinish - taskStart;
                            int priority = Integer.parseInt(
                                    jsonTask.get("container.priority").toString());
                            String type = jsonTask.get("container.type").toString();
                            containerList.add(new ContainerSimulator(containerResource,
                                    lifeTime, hostname, priority, type));
                        }
                    }
                    // create a new AM
                    try {
                        AMSimpleSimulator amSim = (AMSimpleSimulator) ReflectionUtils.newInstance(
                                Class.forName("org.apache.hadoop.yarn.sls.appmaster.PhillyAMSimpleSimulator"), new Configuration());
                        if (amSim != null) {
                            amSim.init(AM_ID++, heartbeatInterval, containerList, rm, yarnClient, fs,
                                    this, jobSubmitTime, jobStartTime, jobFinishTime, user, queue, jobGpu,
                                    isTracked, oldAppId);
                            simpleTimer.scheduleAM(amSim);
                            maxRuntime = Math.max(maxRuntime, jobFinishTime);
                            numTasks += containerList.size();
                            amMap.put(oldAppId, amSim);
                            totalAMCount++;
                        }
                    }
                    catch (Exception e)
                    {
                        LOG.error("PhillyAMSimpleSimulator class not found");
                    }

                    if (jobSubmitTime < smallestJobSubmitTime){
                        smallestJobSubmitTime = jobSubmitTime;
                    }
                }
            } finally {
                input.close();
            }
        }
        simpleTimer.setTotalAM(totalAMCount);
    }

    /**
     * parse workload information from rumen trace files
     */
    @SuppressWarnings("unchecked")
    private void startAMFromRumenTraces(Resource containerResource,
                                        int heartbeatInterval)
            throws IOException {
        // dont support it in SLSSimpleRunner
        /*
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        long baselineTimeMS = 0;
        for (String inputTrace : inputTraces) {
            File fin = new File(inputTrace);
            JobTraceReader reader = new JobTraceReader(
                    new Path(fin.getAbsolutePath()), conf);
            try {
                LoggedJob job = null;
                while ((job = reader.getNext()) != null) {
                    // only support MapReduce currently
                    String jobType = "mapreduce";
                    String user = job.getUser() == null ?
                            "default" : job.getUser().getValue();
                    String jobQueue = job.getQueue().getValue();
                    String oldJobId = job.getJobID().toString();
                    long jobStartTimeMS = job.getSubmitTime();
                    long jobFinishTimeMS = job.getFinishTime();
                    if (baselineTimeMS == 0) {
                        baselineTimeMS = jobStartTimeMS;
                    }
                    jobStartTimeMS -= baselineTimeMS;
                    jobFinishTimeMS -= baselineTimeMS;
                    if (jobStartTimeMS < 0) {
                        LOG.warn("Warning: reset job " + oldJobId + " start time to 0.");
                        jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
                        jobStartTimeMS = 0;
                    }

                    boolean isTracked = trackedApps.contains(oldJobId);
                    int queueSize = queueAppNumMap.containsKey(jobQueue) ?
                            queueAppNumMap.get(jobQueue) : 0;
                    queueSize ++;
                    queueAppNumMap.put(jobQueue, queueSize);

                    List<ContainerSimulator> containerList =
                            new ArrayList<ContainerSimulator>();
                    // map tasks
                    for(LoggedTask mapTask : job.getMapTasks()) {
                        LoggedTaskAttempt taskAttempt = mapTask.getAttempts()
                                .get(mapTask.getAttempts().size() - 1);
                        String hostname = taskAttempt.getHostName().getValue();
                        long containerLifeTime = taskAttempt.getFinishTime()
                                - taskAttempt.getStartTime();
                        containerList.add(new ContainerSimulator(containerResource,
                                containerLifeTime, hostname, 10, "map"));
                    }

                    // reduce tasks
                    for(LoggedTask reduceTask : job.getReduceTasks()) {
                        LoggedTaskAttempt taskAttempt = reduceTask.getAttempts()
                                .get(reduceTask.getAttempts().size() - 1);
                        String hostname = taskAttempt.getHostName().getValue();
                        long containerLifeTime = taskAttempt.getFinishTime()
                                - taskAttempt.getStartTime();
                        containerList.add(new ContainerSimulator(containerResource,
                                containerLifeTime, hostname, 20, "reduce"));
                    }

                    // create a new AM
                    AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
                            amClassMap.get(jobType), conf);
                    if (amSim != null) {
                        // TODO: simply use jobStartTimeMS as traceSubmitTime
                        // we need to fix it later
                        amSim.init(AM_ID ++, heartbeatInterval, containerList,
                                rm, yarnClient, this, jobStartTimeMS, jobStartTimeMS, jobFinishTimeMS, user, jobQueue, 0,
                                isTracked, oldJobId);
                        //runner.schedule(amSim);
                        maxRuntime = Math.max(maxRuntime, jobFinishTimeMS);
                        numTasks += containerList.size();
                        amMap.put(oldJobId, amSim);
                    }
                }
            } finally {
                reader.close();
            }
        }
        */
    }

    private void printSimulationInfo() {
        if (printSimulation) {
            // node
            LOG.info("------------------------------------");
            LOG.info(MessageFormat.format("# nodes = {0}, # racks = {1}, capacity " +
                            "of each node {2} MB memory and {3} vcores.",
                    numNMs, numRacks, nmMemoryMB, nmVCores));
            LOG.info("------------------------------------");
            // job
            LOG.info(MessageFormat.format("# applications = {0}, # total " +
                            "tasks = {1}, average # tasks per application = {2}",
                    numAMs, numTasks, (int)(Math.ceil((numTasks + 0.0) / numAMs))));
            LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
            for (Map.Entry<String, AMSimpleSimulator> entry : amMap.entrySet()) {
                AMSimpleSimulator am = entry.getValue();
                LOG.info(entry.getKey() + "\t" + am.getQueue() + "\t" + am.getAMType()
                        + "\t" + am.getDuration() + "\t" + am.getNumTasks());
            }
            LOG.info("------------------------------------");
            // queue
            LOG.info(MessageFormat.format("number of queues = {0}  average " +
                            "number of apps = {1}", queueAppNumMap.size(),
                    (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size()))));
            LOG.info("------------------------------------");
            // runtime
            LOG.info(MessageFormat.format("estimated simulation time is {0}" +
                    " seconds", (long)(Math.ceil(maxRuntime / 1000.0))));
            LOG.info("------------------------------------");
        }
        // package these information in the simulateInfoMap used by other places
        simulateInfoMap.put("Number of racks", numRacks);
        simulateInfoMap.put("Number of nodes", numNMs);
        simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
        simulateInfoMap.put("Node VCores", nmVCores);
        simulateInfoMap.put("Number of applications", numAMs);
        simulateInfoMap.put("Number of tasks", numTasks);
        simulateInfoMap.put("Average tasks per applicaion",
                (int)(Math.ceil((numTasks + 0.0) / numAMs)));
        simulateInfoMap.put("Number of queues", queueAppNumMap.size());
        simulateInfoMap.put("Average applications per queue",
                (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
        simulateInfoMap.put("Estimated simulate time (s)",
                (long)(Math.ceil(maxRuntime / 1000.0)));
    }

    public HashMap<NodeId, NMSimpleSimulator> getNmMap() {
        return nmMap;
    }

    public static SimpleTimer getSimplerTimer() {
        return simpleTimer;
    }

    public static void decreaseRemainingApps() {
        remainingApps --;

        if (remainingApps == 0) {
            LOG.info("SLSSimpleRunner tears down.");
            System.exit(0);
        }
    }

    public static void main(String args[]) throws Exception {
        Options options = new Options();
        options.addOption("inputrumen", true, "input rumen files");
        options.addOption("inputsls", true, "input sls files");
        options.addOption("nodes", true, "input topology");
        options.addOption("output", true, "output directory");
        options.addOption("trackjobs", true,
                "jobs to be tracked during simulating");
        options.addOption("printsimulation", false,
                "print out simulation information");


        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        String inputRumen = cmd.getOptionValue("inputrumen");
        String inputSLS = cmd.getOptionValue("inputsls");
        String output = cmd.getOptionValue("output");

        if ((inputRumen == null && inputSLS == null) || output == null) {
            System.err.println();
            System.err.println("ERROR: Missing input or output file");
            System.err.println();
            System.err.println("Options: -inputrumen|-inputsls FILE,FILE... " +
                    "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] " +
                    "[-printsimulation]");
            System.err.println();
            System.exit(1);
        }

        File outputFile = new File(output);
        if (! outputFile.exists()
                && ! outputFile.mkdirs()) {
            System.err.println("ERROR: Cannot create output directory "
                    + outputFile.getAbsolutePath());
            System.exit(1);
        }

        Set<String> trackedJobSet = new HashSet<String>();
        if (cmd.hasOption("trackjobs")) {
            String trackjobs = cmd.getOptionValue("trackjobs");
            String jobIds[] = trackjobs.split(",");
            trackedJobSet.addAll(Arrays.asList(jobIds));
        }

        String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

        boolean isSLS = inputSLS != null;
        String inputFiles[] = isSLS ? inputSLS.split(",") : inputRumen.split(",");
        SLSSimpleRunner sls = new SLSSimpleRunner(isSLS, inputFiles, nodeFile, output,
                trackedJobSet, cmd.hasOption("printsimulation"));
        sls.start();
    }
}