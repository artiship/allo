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

package io.github.artiship.allo.worker;

import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.model.ha.ZkScheduler;
import io.github.artiship.allo.rpc.OsUtils;
import io.github.artiship.allo.rpc.RpcClient;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.RpcHeartbeat;
import io.github.artiship.allo.worker.common.CommonCache;
import io.github.com.artiship.ha.SchedulerLeaderRetrieval;
import io.github.com.artiship.ha.utils.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.time.LocalDateTime.now;

@Slf4j
@Component
public class HeartbeatSender implements Service {
    @Autowired private CuratorFramework zkClient;
    @Autowired private SchedulerLeaderRetrieval schedulerLeaderRetrieval;

    @Value("${arlo.worker.rpc.port}")
    private int rpcPort;

    @Value("${arlo.worker.group}")
    private String groups;

    @Value("${arlo.worker.max.task.num}")
    private int maxTask;

    @Value("${arlo.worker.heartbeat.interval}")
    private long heartbeatInterval;

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private RpcHeartbeat heartbeat() {
        return RpcHeartbeat.newBuilder()
                .setHost(OsUtils.getHostIpAddress())
                .setPort(rpcPort)
                .setMaxTask(maxTask)
                .setRunningTasks(CommonCache.currTaskCnt())
                .setCpuUsage(OsUtils.getCpuUsage())
                .setMemoryUsage(OsUtils.getMemoryUsage())
                .setTime(RpcUtils.toProtoTimestamp(now()))
                .addAllWorkerGroups(Arrays.asList(groups.split(",")))
                .build();
    }

    @Override
    public void start() throws Exception {
        executorService.scheduleWithFixedDelay(
                () -> {
                    try {
                        RpcHeartbeat heartbeat = heartbeat();
                        sendToScheduler(heartbeat);
                        sendToZk(heartbeat.getHost(), heartbeat.getPort());
                    } catch (Exception e) {
                        log.error("Send heartbeat failed.", e);
                    }
                },
                0L,
                heartbeatInterval,
                TimeUnit.MILLISECONDS);
    }

    private void sendToScheduler(RpcHeartbeat rpcHeartbeat) throws Exception {
        int count = 0;
        Exception throwE = null;
        long sleepTime = 5L;
        // 5s, 10s, 15
        while (++count <= 3) {
            ZkScheduler leader = schedulerLeaderRetrieval.getLeader();
            try (final RpcClient rpcClient =
                    RpcClient.create(leader.getIp(), leader.getRcpPort())) {
                rpcClient.heartbeat(rpcHeartbeat);
                return;
            } catch (Exception e) {
                throwE = e;
            }

            try {
                TimeUnit.SECONDS.sleep(sleepTime * count);
            } catch (InterruptedException e) {
            }
        }
        throw throwE;
    }

    private void sendToZk(String host, int port) throws Exception {
        CuratorUtils.registerWorker(zkClient, host, port);
    }

    private void removeFromZk() throws Exception {
        CuratorUtils.unRegisterWorker(zkClient, OsUtils.getHostIpAddress(), rpcPort);
    }

    @Override
    public void stop() throws Exception {
        if (null != executorService) {
            executorService.shutdownNow();
        }

        removeFromZk();
    }
}
