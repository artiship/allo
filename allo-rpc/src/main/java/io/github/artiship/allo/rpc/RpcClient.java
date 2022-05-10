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

import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.exception.WorkerBusyException;
import io.github.artiship.allo.rpc.api.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class RpcClient implements AutoCloseable {
    private ManagedChannel channel;
    private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;

    private RpcClient(String host, Integer port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = SchedulerServiceGrpc.newBlockingStub(channel);
    }

    public static RpcClient create(String host, Integer port) {
        return new RpcClient(host, port);
    }

    public static RpcClient create(WorkerBo worker) {
        requireNonNull(worker, "Worker is null");
        requireNonNull(worker.getHost(), "Worker host is null");
        requireNonNull(worker.getPort(), "Worker port is null");

        return new RpcClient(worker.getHost(), worker.getPort());
    }

    public RpcResponse submitTask(RpcTask task) {
        RpcResponse response = stub.submitTask(task);
        if (response.getCode() == 500) {
            throw new WorkerBusyException(response.getMessage());
        }
        return response;
    }

    public RpcResponse killTask(RpcTask task) {
        return stub.killTask(task);
    }

    public RpcResponse updateTask(RpcTask task) {
        return stub.updateTask(task);
    }

    public RpcResponse heartbeat(RpcHeartbeat heartbeat) {
        return stub.heartbeat(heartbeat);
    }

    public HealthCheckResponse healthCheck(HealthCheckRequest request) {
        return stub.healthCheck(request);
    }

    public void shutdown() {
        try {
            if (channel.isShutdown())
                return;

            channel.shutdown()
                   .awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }
}
