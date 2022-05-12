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

package io.github.artiship.allo.worker.api.rpc;

import com.google.common.base.Throwables;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.HealthCheckRequest;
import io.github.artiship.allo.rpc.api.HealthCheckResponse;
import io.github.artiship.allo.rpc.api.RpcResponse;
import io.github.artiship.allo.rpc.api.RpcTask;
import io.github.artiship.allo.rpc.api.SchedulerServiceGrpc.SchedulerServiceImplBase;
import io.github.artiship.allo.worker.api.WorkerBackend;
import io.github.artiship.allo.worker.common.CommonCache;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Future;

/** */
@Component
@Slf4j
public class WorkerRpcService extends SchedulerServiceImplBase {
    @Autowired
    private WorkerBackend workerBackend;

    @Override
    public void submitTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        TaskBo schedulerTaskBo = null;
        try {
            schedulerTaskBo = RpcUtils.toTaskBo(rpcTask);
            workerBackend.submitTask(schedulerTaskBo);
            rpcResponseBuilder.setCode(200);
            CommonCache.addOneTask();
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500).setMessage(subErrorMsg(e));
            log.error(
                    "Task_{}_{}, handle submit task request failed.",
                    rpcTask.getJobId(),
                    rpcTask.getId(),
                    e);
        }
        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void killTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        log.info("Get kill task request, rpcTask [{}]", rpcTask);
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        try {
            Future future = workerBackend.killTask(RpcUtils.toTaskBo(rpcTask));
            future.get();
            rpcResponseBuilder.setCode(200);
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500).setMessage(subErrorMsg(e));
            log.error(
                    String.format(
                            "Task_%s_%s, handle kill task request failed.",
                            rpcTask.getJobId(), rpcTask.getId()),
                    e);
        }

        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void healthCheck(
            HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        HealthCheckResponse healthCheckResponse =
                HealthCheckResponse.newBuilder()
                        .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                        .build();
        responseObserver.onNext(healthCheckResponse);
        responseObserver.onCompleted();
    }

    private String subErrorMsg(Exception e) {
        String allErrorMsg = Throwables.getStackTraceAsString(e);
        return allErrorMsg.length() > 50 ? allErrorMsg.substring(0, 50) : allErrorMsg;
    }
}
