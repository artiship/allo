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
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Lists.reverse;
import static java.lang.Runtime.getRuntime;

@Slf4j
public abstract class ServiceManager {
    private List<Service> services = new ArrayList<>();
    private CountDownLatch isStop = new CountDownLatch(1);

    public void registerAll(Service... list) {
        for (Service service : list) {
            services.add(service);
        }
    }

    protected void start() {
        services.forEach(
                service -> {
                    services.add(service);
                    try {
                        service.start();
                    } catch (Exception e) {
                        log.error("Service Manager start fail", e);
                        stop();
                    }
                });
        getRuntime().addShutdownHook(new Thread(() -> stop()));

        blockUntilShutdown();
    }

    private void blockUntilShutdown() {
        try {
            isStop.await();
        } catch (InterruptedException e) {
            log.error("Service Manager is stop latch await was terminated", e);
        }
    }

    protected void stop() {
        reverse(services)
                .forEach(
                        service -> {
                            try {
                                service.stop();
                            } catch (Exception e) {
                                log.error("Service Manager stop fail", e);
                            }
                        });

        isStop.countDown();
    }
}
