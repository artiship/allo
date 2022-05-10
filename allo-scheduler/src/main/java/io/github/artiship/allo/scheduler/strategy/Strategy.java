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

package io.github.artiship.allo.scheduler.strategy;

import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public interface Strategy {
    Optional<WorkerBo> select(List<WorkerBo> workers, TaskBo task, List<String> lastFailedHosts);

    default Optional<WorkerBo> getWorker(List<WorkerBo> workers,
                                                TaskBo task,
                                                Function<List<WorkerBo>, WorkerBo> strategy,
                                                List<String> lastFailedHosts) {

        List<WorkerBo> selectedWorkers = workers.stream()
                                                       .filter(w -> w.notInFailedList(lastFailedHosts))
                                                       .collect(toList());

        if (selectedWorkers.isEmpty()) {
            selectedWorkers = workers;
        }

        List<WorkerBo> filtered = selectedWorkers.stream()
                                                       .filter(w -> w.isActive())
                                                       .filter(w -> w.hasTaskSlots())
                                                       .filter(w -> w.inWorkerGroups(task.getWorkerGroups()))
                                                       .collect(toList());

        return ofNullable(strategy.apply(filtered));
    }
}