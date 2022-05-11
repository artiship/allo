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
import io.github.artiship.allo.scheduler.rpc.SchedulerRpcServer;
import io.github.com.artiship.ha.ServiceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@SpringBootApplication(
        scanBasePackages = {
            "io.github.artiship.allo.scheduler",
            "io.github.artiship.allo.database"
        })
public class SchedulerLauncher extends ServiceManager implements LeaderElectable {
    @Autowired private JobStateStore jobStateStore;
    @Autowired private TaskDispatcher taskDispatcher;
    @Autowired private DependencyScheduler dependencyScheduler;
    @Autowired private SchedulerRpcServer schedulerRpcServer;
    @Autowired private QuartzScheduler quartzScheduler;
    @Autowired private ZkLeaderElectionAgent zkLeaderElectionAgent;
    @Autowired private ZkWorkerListener zkWorkerListener;
    @Autowired private ZkLostTaskListener zkLostTaskListener;
    @Autowired private RetryScheduler retryScheduler;

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(SchedulerLauncher.class, args);

        SchedulerLauncher app = context.getBean(SchedulerLauncher.class);
        app.register();
        app.start();

        System.exit(SpringApplication.exit(context, () -> 0));
    }

    public void register() {
        registerAll(zkLeaderElectionAgent,
                schedulerRpcServer,
                zkLostTaskListener,
                zkWorkerListener,
                jobStateStore,
                taskDispatcher,
                dependencyScheduler,
                retryScheduler,
                quartzScheduler);
    }

    @Override
    public void electedLeader() {
        log.info("Elected as a leader");
    }

    @Override
    public void revokedLeadership() {
        stop();
    }
}
