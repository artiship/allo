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

package io.github.artiship.allo.scheduler;

import io.github.artiship.allo.scheduler.core.*;
import io.github.artiship.allo.scheduler.ha.LeaderElectable;
import io.github.artiship.allo.scheduler.ha.ZkLeaderElectionAgent;
import io.github.artiship.allo.scheduler.ha.ZkLostTaskListener;
import io.github.artiship.allo.scheduler.ha.ZkWorkerListener;
import io.github.artiship.allo.scheduler.rpc.MasterRpcServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Lists.reverse;
import static java.lang.Runtime.getRuntime;

@Slf4j
@SpringBootApplication(scanBasePackages = {"io.github.artiship.allo.scheduler", "io.github.artiship.allo.database"})
public class SchedulerLauncher implements LeaderElectable {
    private static CountDownLatch isStop = new CountDownLatch(1);

    @Autowired private JobStateStore jobStateStore;
    @Autowired private TaskDispatcher taskDispatcher;
    @Autowired private DependencyScheduler dependencyScheduler;
    @Autowired private MasterRpcServer masterRpcServer;
    @Autowired private QuartzScheduler quartzScheduler;
    @Autowired private ZkLeaderElectionAgent zkLeaderElectionAgent;
    @Autowired private ZkWorkerListener zkWorkerListener;
    @Autowired private ZkLostTaskListener zkLostTaskListener;
    @Autowired private RetryScheduler retryScheduler;

    private List<Service> services = new ArrayList<>();

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(SchedulerLauncher.class, args);
        SchedulerLauncher lau = context.getBean(SchedulerLauncher.class);
        lau.start();

        try {
            isStop.await();
        } catch (InterruptedException e) {
            log.error("Master is stop latch await was terminated", e);
        }

        System.exit(SpringApplication.exit(context, () -> 0));
    }

    public void start() {
        zkLeaderElectionAgent.register(this);
        Arrays.asList(
                        zkLeaderElectionAgent,
                        masterRpcServer,
                        zkLostTaskListener,
                        zkWorkerListener,
                        jobStateStore,
                        taskDispatcher,
                        dependencyScheduler,
                        retryScheduler,
                        quartzScheduler)
                .forEach(
                        service -> {
                            services.add(service);
                            try {
                                service.start();
                            } catch (Exception e) {
                                log.error("Master start fail", e);
                                stop();
                            }
                        });
        getRuntime().addShutdownHook(new Thread(() -> stop()));
    }

    @Override
    public void electedLeader() {
        log.info("Elected as a master");
    }

    @Override
    public void revokedLeadership() {
        stop();
    }

    public void stop() {
        reverse(services)
                .forEach(
                        service -> {
                            try {
                                service.stop();
                            } catch (Exception e) {
                                log.error("Master stop fail", e);
                            }
                        });

        isStop.countDown();
    }
}
