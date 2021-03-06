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
import io.github.artiship.allo.worker.api.WorkerBackend;
import io.github.artiship.allo.worker.api.rpc.WorkerRpcServer;
import io.github.artiship.allo.worker.ha.HeartbeatSender;
import io.github.artiship.allo.worker.ha.DeadWorkerListener;
import io.github.com.artiship.ha.ServiceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
@SpringBootApplication
public class WorkerLauncher extends ServiceManager {
    @Autowired private HeartbeatSender heartbeatSender;
    @Autowired private DeadWorkerListener deadWorkerListener;
    @Autowired private WorkerRpcServer workerRpcServer;
    @Autowired private WorkerBackend workerBackend;

    private static CountDownLatch isStop = new CountDownLatch(1);
    private List<Service> services = new ArrayList<>();

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(WorkerLauncher.class, args);
        WorkerLauncher app = context.getBean(WorkerLauncher.class);
        app.register();
        app.start();
        app.blockUntilShutdown();

        System.exit(SpringApplication.exit(context, () -> 0));
    }

    private void register() {
        registerAll(workerBackend, heartbeatSender, workerRpcServer, deadWorkerListener);
    }
}
