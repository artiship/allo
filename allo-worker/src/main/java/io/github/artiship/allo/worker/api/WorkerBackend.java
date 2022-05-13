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

package io.github.artiship.allo.worker.api;

import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskState;
import io.github.artiship.allo.model.ha.ZkScheduler;
import io.github.artiship.allo.rpc.RpcClient;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.storage.SharedStorage;
import io.github.artiship.allo.worker.executor.AlloExecutor;
import io.github.artiship.allo.worker.executor.LocalAlloExecutor;
import io.github.com.artiship.ha.SchedulerLeaderRetrieval;
import io.github.com.artiship.ha.TaskStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
public class WorkerBackend implements Service, TaskStateListener {
    @Autowired private SharedStorage sharedStorage;
    @Autowired private SchedulerLeaderRetrieval schedulerLeaderRetrieval;

    @Value("${worker.max.task.num}")
    private int maxTaskNum;

    @Value("${worker.task.local.base.path}")
    private String taskLocalBasePath;

    private Map<Long, AlloExecutor> executors = new ConcurrentHashMap<>();
    private Set<Long> failOverTasks = new HashSet<>();

    private ExecutorService executorService;

    @Override
    public void start() throws Exception {
        executorService = Executors.newFixedThreadPool(maxTaskNum * 3);
    }

    public void submitTask(TaskBo task) {
        executorService.submit(
                () -> {
                    LocalAlloExecutor executor =
                            new LocalAlloExecutor(task, sharedStorage, taskLocalBasePath);

                    executors.put(task.getId(), executor);
                    executor.execute();
                });
    }

    private void reportScheduler(TaskBo task) {
        ZkScheduler leader = this.schedulerLeaderRetrieval.getLeader();
        RpcClient.create(leader.getIp(), leader.getRcpPort()).updateTask(RpcUtils.toRpcTask(task));
    }

    public Future killTask(TaskBo task) {
        return null;
    }

    public void failOver() {
        executors.entrySet().stream().forEach(entry -> {
            failOverTasks.add(entry.getKey());
            AlloExecutor executor = entry.getValue();
            executor.kill();
        });
    }

    @Override
    public void onRunning(TaskBo task) {
        reportScheduler(task);
    }

    @Override
    public void onKilled(TaskBo task) {
        if (failOverTasks.contains(task.getId())) {
            task.setTaskState(TaskState.FAIL_OVER);
        }
        reportScheduler(task);
        executors.remove(task.getId());
    }

    @Override
    public void onSuccess(TaskBo task) {
        if (failOverTasks.contains(task.getId())) {
            task.setTaskState(TaskState.FAIL_OVER);
        }
        reportScheduler(task);
        executors.remove(task.getId());
    }

    @Override
    public void onFail(TaskBo task) {
        if (failOverTasks.contains(task.getId())) {
            task.setTaskState(TaskState.FAIL_OVER);
        }
        reportScheduler(task);
        executors.remove(task.getId());
    }

    @Override
    public void onFailOver(TaskBo task) {
        reportScheduler(task);
        executors.remove(task.getId());
    }

    @Override
    public void stop() throws Exception {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
