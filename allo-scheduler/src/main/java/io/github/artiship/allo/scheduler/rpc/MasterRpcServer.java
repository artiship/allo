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

package io.github.artiship.allo.scheduler.rpc;

import io.github.artiship.allo.scheduler.core.Service;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.grpc.ServerBuilder.forPort;

@Slf4j
@Component
public class MasterRpcServer implements Service {
    @Value("${rpc.port:9090}")
    private int DEFAULT_PORT = 9090;

    @Autowired
    private MasterRpcService masterRpcService;
    private Server server;

    @Override
    public void start() {
        try {
            server = forPort(DEFAULT_PORT).addService(masterRpcService)
                                          .build()
                                          .start();
        } catch (IOException e) {
            log.info("Rpc server start failed", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.shutdown()
                      .awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.info("Rpc server stop failed", e);
            }
        }
    }
}