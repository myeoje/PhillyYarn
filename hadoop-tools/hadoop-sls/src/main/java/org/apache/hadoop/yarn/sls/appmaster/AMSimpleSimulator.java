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

package org.apache.hadoop.yarn.sls.appmaster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords
        .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;

import org.apache.hadoop.yarn.api.protocolrecords
        .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
        .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.sls.SLSSimpleRunner;
import org.apache.hadoop.yarn.sls.scheduler.SimpleTimer;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import sun.java2d.pipe.SpanShapeRenderer;

@Private
@Unstable
public abstract class AMSimpleSimulator {
    // resource manager
    protected ResourceManager rm;
    protected YarnClient yarnClient;
    // main
    protected SLSSimpleRunner se;
    // application
    protected ApplicationId appId;
    protected ApplicationAttemptId appAttemptId;
    protected String oldAppId;    // jobId from the jobhistory file
    // record factory
    protected final static RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);
    // response queue
    protected final BlockingQueue<AllocateResponse> responseQueue;
    protected int RESPONSE_ID = 1;
    // user name
    protected String user;
    // queue name
    protected String queue;
    // jobGpu number:  for philly
    protected int jobGpu;
    // am type
    protected String amtype;
    // job start/end time
    protected long traceSubmitTimeMS;
    protected long traceStartTimeMS;
    protected long traceFinishTimeMS;
    protected long simulateSubmitTimeMS;
    protected long simulateStartTimeMS;
    protected long simulateFinishTimeMS;
    private long heartBeatInterval;
    // whether tracked in Metrics
    protected boolean isTracked;
    // progress
    protected int totalContainers;
    protected int finishedContainers;

    protected boolean isAMFinished;
    private boolean doneWithLastStep;

    protected final Logger LOG = Logger.getLogger(AMSimpleSimulator.class);

    public AMSimpleSimulator() {
        this.responseQueue = new LinkedBlockingQueue<AllocateResponse>();
    }

    public void init(int id, long heartbeatInterval,
                     List<ContainerSimulator> containerList, ResourceManager rm, YarnClient yarnClient, SLSSimpleRunner se,
                     long traceSubmitTime, long traceStartTime, long traceFinishTime, String user, String queue, int jobGpu,
                     boolean isTracked, String oldAppId) {
        // TODO end time is not big enough
        //super.init(traceSubmitTime, traceStartTime + 1000000L * heartbeatInterval, heartbeatInterval);

        // 3 month
        //super.init(traceSubmitTime, traceSubmitTime + 3 * 31 * 24 * 60 * 60 * heartbeatInterval, heartbeatInterval);
        this.isAMFinished = false;
        this.doneWithLastStep = false;
        this.heartBeatInterval = heartbeatInterval;
        this.user = user;
        this.rm = rm;
        this.yarnClient = yarnClient;
        this.se = se;
        this.user = user;
        this.queue = queue;
        this.oldAppId = oldAppId;
        this.isTracked = isTracked;
        this.traceSubmitTimeMS = traceSubmitTime;
        this.traceStartTimeMS = traceStartTime;
        this.traceFinishTimeMS = traceFinishTime;
        this.jobGpu = jobGpu;
    }
    public int getHeartBeatIntervalSecond()
    {
        int res = (int)(heartBeatInterval / 1000);
        if (res == 0) res = 1;
        return res;
    }
    public int getTraceSubmitTimeSecond()
    {
        int res = (int)(traceSubmitTimeMS / 1000);
        if (res == 0) res = 1;
        return res;
    }

    public boolean amIDoneWithLastStep()
    {
        return doneWithLastStep;
    }


    public void run() {
        try {
            if (SimpleTimer.currentTime == getTraceSubmitTimeSecond()) {
                firstStep();
            } else {
                if (!isAMFinished) {
                    middleStep();
                } else {
                    lastStep();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Thread.getDefaultUncaughtExceptionHandler()
                    .uncaughtException(Thread.currentThread(), e);
        }
    }

    /**
     * register with RM
     */

    public void firstStep() throws Exception {
        simulateSubmitTimeMS = SimpleTimer.currentTime * 1000;

        // submit application, waiting until ACCEPTED
        submitApp();

        // register application master
        registerAM();

        // track app metrics
        trackApp();
    }


    public void middleStep() throws Exception {
        long currentTimeMS = SimpleTimer.currentTime * 1000;
        //LOG.info(MessageFormat.format("middle step: {0}", currentTimeMS - SLSRunner.getRunner().getStartTimeMS()));
        // process responses in the queue
        processResponseQueue(currentTimeMS);

        // send out request
        sendContainerRequest(currentTimeMS);

        // check whether finish
        checkStop(currentTimeMS);

        // check whether resource requiring timeout and yielding timeout
        checkTimeOut(currentTimeMS);
    }


    public void lastStep() throws Exception {
        long currentTimeMS = SimpleTimer.currentTime * 1000;
        LOG.info(MessageFormat.format("Application {0} is shutting down.", appId));
        // unregister tracking
        if (isTracked) {
            untrackApp();
        }
        // unregister application master
        final FinishApplicationMasterRequest finishAMRequest = recordFactory
                .newRecordInstance(FinishApplicationMasterRequest.class);
        finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

        UserGroupInformation ugi =
                UserGroupInformation.createRemoteUser(appAttemptId.toString());
        Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps().get(appId)
                .getRMAppAttempt(appAttemptId).getAMRMToken();
        ugi.addTokenIdentifier(token.decodeIdentifier());
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                rm.getApplicationMasterService()
                        .finishApplicationMaster(finishAMRequest);
                return null;
            }
        });

        simulateFinishTimeMS = currentTimeMS;
        // record job running information
        ((ResourceSchedulerWrapper)rm.getResourceScheduler())
                .addAMRuntime(oldAppId, appId,
                        traceSubmitTimeMS, traceStartTimeMS, traceFinishTimeMS,
                        simulateSubmitTimeMS, simulateStartTimeMS, simulateFinishTimeMS);
        doneWithLastStep = true;
    }

    protected ResourceRequest createResourceRequest(
            Resource resource, String host, int priority, int numContainers, boolean relaxLocality) {
        ResourceRequest request = recordFactory
                .newRecordInstance(ResourceRequest.class);
        request.setCapability(resource);
        request.setResourceName(host);
        request.setNumContainers(numContainers);
        Priority prio = recordFactory.newRecordInstance(Priority.class);
        prio.setPriority(priority);
        request.setPriority(prio);
        request.setRelaxLocality(relaxLocality);
        return request;
    }

    protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
                                                    List<ContainerId> toRelease) {
        AllocateRequest allocateRequest =
                recordFactory.newRecordInstance(AllocateRequest.class);
        allocateRequest.setResponseId(RESPONSE_ID ++);
        allocateRequest.setAskList(ask);
        allocateRequest.setReleaseList(toRelease);
        return allocateRequest;
    }

    protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
        return createAllocateRequest(ask, new ArrayList<ContainerId>());
    }

    protected AllocateRequest createReleaseRequest(List<ContainerId> toRelease) {
        return createAllocateRequest(new ArrayList<ResourceRequest>(), toRelease);
    }

    protected abstract void checkTimeOut(long currentTimeMS) throws Exception;

    protected abstract void processResponseQueue(long currentTimeMS) throws Exception;

    protected abstract void sendContainerRequest(long currentTimeMS) throws Exception;

    protected abstract void checkStop(long currentTimeMS);

    private void submitApp()
            throws YarnException, InterruptedException, IOException {
        // ask for new application
        GetNewApplicationRequest newAppRequest =
                Records.newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse newAppResponse =
                rm.getClientRMService().getNewApplication(newAppRequest);
        appId = newAppResponse.getApplicationId();

        // submit the application
        final SubmitApplicationRequest subAppRequest =
                Records.newRecord(SubmitApplicationRequest.class);
        ApplicationSubmissionContext appSubContext =
                Records.newRecord(ApplicationSubmissionContext.class);
        appSubContext.setApplicationId(appId);
        appSubContext.setMaxAppAttempts(1);
        appSubContext.setQueue(queue);
        appSubContext.setPriority(Priority.newInstance(0));
        ContainerLaunchContext conLauContext =
                Records.newRecord(ContainerLaunchContext.class);
        conLauContext.setApplicationACLs(
                new HashMap<ApplicationAccessType, String>());
        conLauContext.setCommands(new ArrayList<String>());
        conLauContext.setEnvironment(new HashMap<String, String>());
        conLauContext.setLocalResources(new HashMap<String, LocalResource>());
        conLauContext.setServiceData(new HashMap<String, ByteBuffer>());
        appSubContext.setAMContainerSpec(conLauContext);
        appSubContext.setUnmanagedAM(true);
        subAppRequest.setApplicationSubmissionContext(appSubContext);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws YarnException {
                rm.getClientRMService().submitApplication(subAppRequest);
                return null;
            }
        });
        LOG.info(MessageFormat.format("Submit a new application {0}", appId));

        // waiting until application ACCEPTED
        RMApp app = rm.getRMContext().getRMApps().get(appId);
        while(app.getState() != RMAppState.ACCEPTED) {
            Thread.sleep(10);
        }

        // Waiting until application attempt reach LAUNCHED
        // "Unmanaged AM must register after AM attempt reaches LAUNCHED state"
        this.appAttemptId = rm.getRMContext().getRMApps().get(appId)
                .getCurrentAppAttempt().getAppAttemptId();
        RMAppAttempt rmAppAttempt = rm.getRMContext().getRMApps().get(appId)
                .getCurrentAppAttempt();
        while (rmAppAttempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED) {
            Thread.sleep(10);
        }
    }

    private void registerAM()
            throws YarnException, IOException, InterruptedException {
        // register application master
        final RegisterApplicationMasterRequest amRegisterRequest =
                Records.newRecord(RegisterApplicationMasterRequest.class);
        amRegisterRequest.setHost("localhost");
        amRegisterRequest.setRpcPort(1000);
        amRegisterRequest.setTrackingUrl("localhost:1000");

        UserGroupInformation ugi =
                UserGroupInformation.createRemoteUser(appAttemptId.toString());
        Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps().get(appId)
                .getRMAppAttempt(appAttemptId).getAMRMToken();
        ugi.addTokenIdentifier(token.decodeIdentifier());

        ugi.doAs(
                new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
                    @Override
                    public RegisterApplicationMasterResponse run() throws Exception {
                        return rm.getApplicationMasterService()
                                .registerApplicationMaster(amRegisterRequest);
                    }
                });

        LOG.info(MessageFormat.format(
                "Register the application master for application {0}", appId));
    }

    private void trackApp() {
        if (isTracked) {
            ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                    .addTrackedApp(appAttemptId, oldAppId);
        }
    }
    public void untrackApp() {
        if (isTracked) {
            ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                    .removeTrackedApp(appAttemptId, oldAppId);
        }
    }

    protected List<ResourceRequest> packageRequests(
            List<ContainerSimulator> csList, int priority, boolean relaxLocality) {
        // Wencong: assume all callers share the same relaxLocality
        // it's "false" for Philly jobs
        // it's "true" for MR jobs

        // create requests
        Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
        Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
        ResourceRequest anyRequest = null;
        for (ContainerSimulator cs : csList) {
            String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
            // check rack local
            String rackname = rackHostNames[0];
            if (rackLocalRequestMap.containsKey(rackname)) {
                rackLocalRequestMap.get(rackname).setNumContainers(
                        rackLocalRequestMap.get(rackname).getNumContainers() + 1);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), rackname, priority, 1, relaxLocality);
                rackLocalRequestMap.put(rackname, request);
            }
            // check node local
            String hostname = rackHostNames[1];
            if (nodeLocalRequestMap.containsKey(hostname)) {
                nodeLocalRequestMap.get(hostname).setNumContainers(
                        nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), hostname, priority, 1, relaxLocality);
                nodeLocalRequestMap.put(hostname, request);
            }
            // any
            if (anyRequest == null) {
                anyRequest = createResourceRequest(
                        cs.getResource(), ResourceRequest.ANY, priority, 1, relaxLocality);
            } else {
                anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
            }
        }
        List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
        ask.addAll(nodeLocalRequestMap.values());
        ask.addAll(rackLocalRequestMap.values());
        if (anyRequest != null) {
            ask.add(anyRequest);
        }
        LOG.debug(MessageFormat.format("Application {0} is sending resource request: {1}",appId,  ask));
        return ask;
    }


    protected List<ResourceRequest> packageRequests(
            List<ContainerSimulator> csList, int priority, String rack, String nodeList, boolean relaxLocality) {
        // Wencong: assume all callers share the same relaxLocality
        // it's "false" for Philly jobs
        // it's "true" for MR jobs

        // this function is only for Philly jobs
        // assume all container share the same node-level locality requirement and don't relax locality

        // create requests
        Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
        Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
        ResourceRequest anyRequest = null;
        for (ContainerSimulator cs : csList) {
            //String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
            // check rack local
            String rackname = rack;
            if (rackLocalRequestMap.containsKey(rackname)) {
                rackLocalRequestMap.get(rackname).setNumContainers(
                        rackLocalRequestMap.get(rackname).getNumContainers() + 1);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), rackname, priority, 1, relaxLocality);
                rackLocalRequestMap.put(rackname, request);
            }
            // check node local
            String hostname = nodeList;
            if (nodeLocalRequestMap.containsKey(hostname)) {
                nodeLocalRequestMap.get(hostname).setNumContainers(
                        nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), hostname, priority, 1, relaxLocality);
                nodeLocalRequestMap.put(hostname, request);
            }
            // any
            if (anyRequest == null) {
                anyRequest = createResourceRequest(
                        cs.getResource(), ResourceRequest.ANY, priority, 1, relaxLocality);
            } else {
                anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
            }
        }
        List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
        ask.addAll(nodeLocalRequestMap.values());
        ask.addAll(rackLocalRequestMap.values());
        if (anyRequest != null) {
            ask.add(anyRequest);
        }
        LOG.debug(MessageFormat.format("Application {0} is sending resource request: {1}",appId,  ask));
        return ask;

    }

    protected List<ResourceRequest> cancelRequests(
            List<ContainerSimulator> csList, int priority, String rack, String nodeList, boolean relaxLocality) {
        // create requests
        Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
        Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
        ResourceRequest anyRequest = null;
        for (ContainerSimulator cs : csList) {
            //String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
            // check rack local
            String rackname = rack;
            if (rackLocalRequestMap.containsKey(rackname)) {
                rackLocalRequestMap.get(rackname).setNumContainers(0);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), rackname, priority, 0, relaxLocality);
                rackLocalRequestMap.put(rackname, request);
            }
            // check node local
            String hostname = nodeList;
            if (nodeLocalRequestMap.containsKey(hostname)) {
                nodeLocalRequestMap.get(hostname).setNumContainers(
                        nodeLocalRequestMap.get(hostname).getNumContainers() - 1);
            } else {
                ResourceRequest request = createResourceRequest(
                        cs.getResource(), hostname, priority, -1, relaxLocality);
                nodeLocalRequestMap.put(hostname, request);
            }
            // any
            if (anyRequest == null) {
                anyRequest = createResourceRequest(
                        cs.getResource(), ResourceRequest.ANY, priority, 0, relaxLocality);
            } else {
                anyRequest.setNumContainers(0);
            }
        }
        List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
        ask.addAll(nodeLocalRequestMap.values());
        ask.addAll(rackLocalRequestMap.values());
        if (anyRequest != null) {
            ask.add(anyRequest);
        }
        LOG.debug(MessageFormat.format("Application {0} is sending resource request: {1}",appId,  ask));
        return ask;
    }

    public String getQueue() {
        return queue;
    }
    public String getAMType() {
        return amtype;
    }
    public long getDuration() {
        return simulateFinishTimeMS - simulateStartTimeMS;
    }
    public int getNumTasks() {
        return totalContainers;
    }
}
