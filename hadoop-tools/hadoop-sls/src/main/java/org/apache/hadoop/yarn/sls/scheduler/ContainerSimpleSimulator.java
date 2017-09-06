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

package org.apache.hadoop.yarn.sls.scheduler;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.sls.SLSRunner;

@Private
@Unstable
public class ContainerSimpleSimulator{
    // id
    private ContainerId id;
    // resource allocated
    private Resource resource;
    // end time
    private long endTime;
    // life time (ms)
    private long lifeTime;
    // host name
    private String hostname;
    // priority
    private int priority;
    // type
    private String type;

    /**
     * invoked when AM schedules containers to allocate
     */
    public ContainerSimpleSimulator(Resource resource, long lifeTime,
                                    String hostname, int priority, String type) {
        this.resource = resource;
        this.lifeTime = lifeTime;
        this.hostname = hostname;
        this.priority = priority;
        this.type = type;
    }

    /**
     * invoke when NM schedules containers to run
     */
    public ContainerSimpleSimulator(ContainerId id, Resource resource, long endTime,
                                    long lifeTime) {
        this.id = id;
        this.resource = resource;
        this.endTime = endTime;
        this.lifeTime = lifeTime;
    }

    public Resource getResource() {
        return resource;
    }

    public ContainerId getId() {
        return id;
    }

    public long getLifeTime() {
        return lifeTime;
    }

    public String getHostname() {
        return hostname;
    }

    public long getEndTime() {
        return endTime;
    }

    public int getPriority() {
        return priority;
    }

    public String getType() {
        return type;
    }

    public void setPriority(int p) {
        priority = p;
    }
}