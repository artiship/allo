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

package io.github.artiship.allo.scheduler.core;

import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.scheduler.strategy.DefaultStrategy;
import io.github.artiship.allo.scheduler.strategy.Strategy;
import io.github.artiship.allo.scheduler.strategy.WorkerSelectStrategyEnum;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class WorkerSelector {

    private static Map<WorkerSelectStrategyEnum, Strategy> strategies = new ConcurrentHashMap<>();

    static {
        strategies.put(WorkerSelectStrategyEnum.DEFAULT, new DefaultStrategy());
    }

    private Strategy strategy;
    private List<WorkerBo> workers;
    private TaskBo task;
    private List<String> lastFailedHosts;

    private WorkerSelector() {

    }

    public static WorkerSelector builder() {
        return new WorkerSelector();
    }

    public WorkerSelector withStrategy(Strategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public WorkerSelector withWorkers(List<WorkerBo> workers) {
        this.workers = workers;
        return this;
    }

    public WorkerSelector withLastFailedHosts(List<String> lastFailedHosts) {
        this.lastFailedHosts = lastFailedHosts;
        return this;
    }

    public WorkerSelector withTask(TaskBo task) {
        this.task = task;
        return this;
    }

    public Optional<WorkerBo> select() {
        requireNonNull(task, "Task is null");
        requireNonNull(workers, "Workers is null");

        if (lastFailedHosts == null) {
            lastFailedHosts = emptyList();
        }

        if (strategy == null) {
            strategy = strategies.get(WorkerSelectStrategyEnum.DEFAULT);
        }

        return strategy.select(workers, task, lastFailedHosts);
    }
}
