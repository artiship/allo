/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.com.artiship.ha;

import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.model.ha.ZkScheduler;
import io.github.com.artiship.ha.utils.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import static io.github.com.artiship.ha.GlobalConstants.SCHEDULER_GROUP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;

@Slf4j
public class SchedulerLeaderRetrieval implements NodeCacheListener, Service {
    private final CuratorFramework zkClient;

    private NodeCache masterCache;
    private ZkScheduler leader;

    public SchedulerLeaderRetrieval(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void start() throws Exception {
        try {
            masterCache =
                    new NodeCache(zkClient, CuratorUtils.createPath(zkClient, SCHEDULER_GROUP));
            masterCache.start(true);
            masterCache.getListenable().addListener(this);

            retrieve();
        } catch (Exception e) {
            log.info("Start cache zk master node {} fail", SCHEDULER_GROUP, e);
        }
    }

    @Override
    public void nodeChanged() {
        retrieve();
    }

    private void retrieve() {
        leader = ZkScheduler.from(new String(masterCache.getCurrentData().getData(), UTF_8));
    }

    public ZkScheduler getLeader() {
        return this.leader;
    }

    public String getMasterHttpUrl() {
        return new StringBuffer("http://")
                .append(leader.getIp())
                .append(":")
                .append(leader.getHttpPort())
                .toString();
    }

    @Override
    public void stop() throws Exception {
        if (zkClient.getState() == STARTED) {
            zkClient.close();
        }
    }
}
