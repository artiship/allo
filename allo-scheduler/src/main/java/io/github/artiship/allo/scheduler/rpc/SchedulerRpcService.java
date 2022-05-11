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

import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskOperation;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.RpcHeartbeat;
import io.github.artiship.allo.rpc.api.RpcResponse;
import io.github.artiship.allo.rpc.api.RpcTask;
import io.github.artiship.allo.rpc.api.SchedulerServiceGrpc;
import io.github.artiship.allo.scheduler.core.*;
import io.github.com.artiship.ha.TaskStateListener;
import io.github.com.artiship.ha.TaskStateNotifier;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static io.github.artiship.allo.model.enums.TaskState.finishStates;

@Slf4j
@Service
public class SchedulerRpcService extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    @Autowired
    private ResourceManager resourceManager;
    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private TaskOperationCache taskOperationCache;

    private TaskStateNotifier taskStateNotifier = new TaskStateNotifier();

    @Override
    public void heartbeat(RpcHeartbeat heartbeat, StreamObserver<RpcResponse> responseStreamObserver) {
        RpcResponse.Builder response = RpcResponse.newBuilder().setCode(200);

        log.debug("Receive worker {} heartbeat.", heartbeat.getHost());
        resourceManager.updateWorker(RpcUtils.toWorkerBo(heartbeat));

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }

    @Override
    public void updateTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseStreamObserver) {
        RpcResponse.Builder response = RpcResponse.newBuilder().setCode(200);
        try {
            if (finishStates().contains(schedulerDao.getTaskStateById(rpcTask.getId()))) {
                return;
            }

            TaskBo task = schedulerDao.saveTask(RpcUtils.toTaskBo(rpcTask));

            log.info("{} {} on worker {}: app_id={}, pid={}",
                    task.traceId(),
                    task.getTaskState(),
                    task.getWorkerHost(),
                    task.getApplicationIds(),
                    task.getPid());

            switch (task.getTaskState()) {
                case RUNNING:
                    taskStateNotifier.notifyRunning(task);
                    break;
                case SUCCESS:
                    taskStateNotifier.notifySuccess(task);
                    break;
                case FAIL:
                    taskStateNotifier.notifyFail(task);
                    break;
                case KILLED:
                    if (taskOperationCache.applied(task, TaskOperation.MARK_SUCCESS)) {
                        taskStateNotifier.notifySuccess(task);
                    } else if (taskOperationCache.applied(task, TaskOperation.MARK_FAIL)) {
                        taskStateNotifier.notifyFail(task);
                    } else {
                        taskStateNotifier.notifyKilled(task);
                    }
                    break;
                case FAIL_OVER:
                    taskStateNotifier.notifyFailOver(task);
                default:
                    log.warn("{} state {} is not handled", task.traceId(), task.getTaskState());
                    break;
            }
        } catch (Exception e) {
            log.warn("Task_{}_{} update fail", rpcTask.getJobId(), rpcTask.getId(), e);
        }

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }

    public void registerListener(TaskStateListener listener) {
        taskStateNotifier.registerListener(listener);
    }
}
