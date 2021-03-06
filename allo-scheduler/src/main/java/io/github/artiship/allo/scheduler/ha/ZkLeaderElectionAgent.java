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

package io.github.artiship.allo.scheduler.ha;

import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.ha.ZkScheduler;
import io.github.artiship.allo.rpc.OsUtils;
import io.github.artiship.allo.scheduler.core.SchedulerDao;
import io.github.artiship.allo.common.Service;
import io.github.com.artiship.ha.utils.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static io.github.com.artiship.ha.GlobalConstants.SCHEDULER_GROUP;
import static io.github.artiship.allo.model.enums.WorkerState.ACTIVE;
import static io.github.artiship.allo.model.enums.WorkerState.STANDBY;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STOPPED;

@Slf4j
@Component
public class ZkLeaderElectionAgent implements LeaderLatchListener, Service, ConnectionStateListener {
    @Resource
    private CuratorFramework zkClient;
    @Autowired
    private SchedulerDao schedulerDao;

    @Value("${server.port}")
    private int http_port;
    @Value("${rpc.port}")
    private int rpc_port = 9090;

    private LeaderLatch leaderLatch;
    private LeadershipStatus status = LeadershipStatus.NOT_LEADER;

    private LeaderElectable master;

    public void register(LeaderElectable master) {
        this.master = master;
    }

    @Override
    public void start() throws Exception {
        zkClient.getZookeeperClient().blockUntilConnectedOrTimedOut();
        zkClient.getConnectionStateListenable()
                .addListener(this);

        leaderLatch = new LeaderLatch(zkClient, CuratorUtils.createPath(zkClient, SCHEDULER_GROUP));
        leaderLatch.addListener(this);
        leaderLatch.start();
        leaderLatch.await();
    }

    @Override
    public void stop() throws IOException {
        if (zkClient.getState() == STOPPED)
            return;

        if (leaderLatch.getState() == LeaderLatch.State.STARTED) {
            leaderLatch.close();
        }

        if (zkClient.getState() == STARTED) {
            zkClient.close();
        }
    }

    @Override
    public void isLeader() {
        synchronized (this) {
            // could have lost leadership by now.
            if (!leaderLatch.hasLeadership()) {
                return;
            }

            try {
                ZkScheduler zkScheduler = new ZkScheduler(OsUtils.getHostIpAddress(), rpc_port, http_port);
                zkClient.setData()
                        .forPath(SCHEDULER_GROUP, zkScheduler.toString()
                                                       .getBytes(UTF_8));
            } catch (Exception e) {
                log.error("Set data failed", e);
            }

            log.info("We have gained leadership");
            updateLeadershipStatus(true);
        }
    }

    @Override
    public void notLeader() {
        synchronized (this) {
            // could have gained leadership by now.
            if (leaderLatch.hasLeadership()) {
                return;
            }

            log.info("We have lost leadership");
            updateLeadershipStatus(false);
        }
    }

    private void updateLeadershipStatus(boolean isLeader) {
        if (isLeader && status == LeadershipStatus.NOT_LEADER) {
            status = LeadershipStatus.LEADER;
            master.electedLeader();
        } else if (!isLeader && status == LeadershipStatus.LEADER) {
            status = LeadershipStatus.NOT_LEADER;
            master.revokedLeadership();
        }
    }

    @Override
    public void stateChanged(CuratorFramework zkClient, ConnectionState state) {
        switch (state) {
            case SUSPENDED:
            case RECONNECTED:
            case LOST:
            case READ_ONLY:
            case CONNECTED:
        }

        log.info("Zookeeper state change to {}", state);
    }

    private enum LeadershipStatus {
        LEADER, NOT_LEADER;
    }

    private void updateDb() {
        try {
            schedulerDao.saveNode(new WorkerBo()
                    .setHost(OsUtils.getHostIpAddress())
                    .setPort(this.rpc_port)
                    .setWorkerState(status == LeadershipStatus.LEADER ? ACTIVE : STANDBY));
        } catch (Exception e) {
            log.warn("Save master info to db fail.", e);
        }
    }
}
