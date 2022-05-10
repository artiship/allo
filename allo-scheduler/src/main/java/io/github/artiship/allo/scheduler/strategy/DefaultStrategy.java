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

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

/**
 * mem < 90% & max(availableSlots = max_tasks - running_tasks)
 */
public class DefaultStrategy implements Strategy {

    public Optional<WorkerBo> select(List<WorkerBo> list,
                                            TaskBo task,
                                            List<String> lastFailedHosts) {
        return getWorker(list,
                task,
                workers -> workers.stream()
                                  .filter(w -> memUsageUnder90percent(w.getMemoryUsage()))
                                  .sorted(comparing(WorkerBo::availableSlots, reverseOrder()))
                                  .findFirst()
                                  .orElse(null),
                lastFailedHosts);
    }

    private boolean memUsageUnder90percent(Double memUsage) {
        if (memUsage == null) return true;
        return memUsage < 90.0;
    }
}