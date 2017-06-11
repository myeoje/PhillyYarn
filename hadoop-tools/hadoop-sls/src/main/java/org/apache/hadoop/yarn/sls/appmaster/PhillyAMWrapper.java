package org.apache.hadoop.yarn.sls.appmaster;

import java.text.MessageFormat;
import java.util.Random;

import com.microsoft.philly.appmaster.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.log4j.Logger;


/**
 * Created by wencong on 17-5-2.
 */
public class PhillyAMWrapper {

    private TopologyAwareNodeAllocator topo;

    public final Logger LOG = Logger.getLogger(PhillyAMWrapper.class);

    /** Sets the min timeout for containers in Allocating state */
    final static int MinAllocationTimeoutSeconds = 120;

    /** Sets the max timeout for containers in Allocating state */
    final static int MaxAllocationTimeoutSeconds = 180;

    /** Sets the min time to spend in Yield state (between allocation attempts) */
    final static int MinYieldTimeSeconds = 150;

    /** Sets the max time to spend in Yield state (between allocation attempts) */
    final static int MaxYieldTimeSeconds = 200;

    private Random random;

    public PhillyAMWrapper(YarnClient yarnClient, AppMasterContainer appMasterContainer)
    {
        topo = new TopologyAwareNodeAllocator(yarnClient, appMasterContainer, null);
        random = new Random(System.currentTimeMillis());
    }
    public TopologyAwareNodeAllocator.NodeGroup getBestNodes(ContainerResourceDescriptor containerDesc)
    {
        // return locality preference
        TopologyAwareNodeAllocator.NodeGroup nodeGroup = null;
        try {
            // TODO: parameter
            nodeGroup = topo.getBestNodes(containerDesc);
        }
        catch(Exception e){
            LOG.debug("topologyAwareNodeAllocator getBestNodesForSLS failed");
        }
        return nodeGroup;
    }

    public long getAllocatingTimeout() {
        // directly copy from philly-am
        int allocationTimeoutSeconds = random.nextInt(MaxAllocationTimeoutSeconds - MinAllocationTimeoutSeconds) + MinAllocationTimeoutSeconds;
        return (long)allocationTimeoutSeconds * 1000L;
    }

    public long getYieldingTimeout() {
        // directly copy from philly-am
        int yieldTimeoutSeconds = MinYieldTimeSeconds + random.nextInt(MaxYieldTimeSeconds - MinYieldTimeSeconds);
        return (long)yieldTimeoutSeconds * 1000L;
    }
}
