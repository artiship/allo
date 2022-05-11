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

package io.github.artiship.allo.rpc;

import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.rpc.api.SchedulerServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class RpcServer implements Service {
    private Server server;

    @Override
    public void start() {
        try {
            server = ServerBuilder.forPort(getRpcPort()).addService(getRpcService()).build().start();
        } catch (IOException e) {
            log.info("Rpc server start failed", e);
        }
    }

    protected abstract int getRpcPort();

    protected abstract SchedulerServiceGrpc.SchedulerServiceImplBase getRpcService();

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.info("Rpc server stop failed", e);
            }
        }
    }
}
