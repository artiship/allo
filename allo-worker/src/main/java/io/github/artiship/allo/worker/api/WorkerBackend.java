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
import io.github.artiship.allo.storage.SharedStorage;
import io.github.artiship.allo.worker.executor.LocalExecutor;
import io.github.com.artiship.ha.TaskStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
public class WorkerBackend implements Service, TaskStateListener {
    @Autowired private SharedStorage sharedStorage;

    @Value("${worker.max.task.num}")
    private int maxTaskNum;

    @Value("${worker.task.local.base.path}")
    private String taskLocalBasePath;

    private ExecutorService executorService;

    @Override
    public void start() throws Exception {
        executorService = Executors.newFixedThreadPool(maxTaskNum * 3);
    }

    public void submitTask(TaskBo task) {
        executorService.submit(
                () -> {
                    LocalExecutor executor =
                            new LocalExecutor(task, sharedStorage, taskLocalBasePath);

                    executor.execute();
                });
    }

    public Future killTask(TaskBo task) {
        return null;
    }

    @Override
    public void onRunning(TaskBo task) {}

    @Override
    public void onKilled(TaskBo task) {}

    @Override
    public void onSuccess(TaskBo task) {}

    @Override
    public void onFail(TaskBo task) {}

    @Override
    public void onFailOver(TaskBo task) {}

    @Override
    public void stop() throws Exception {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
