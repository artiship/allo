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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static java.util.concurrent.TimeUnit.HOURS;

@Slf4j
@Component
public class TaskOperationCache {
    protected Cache<Long, TaskOperation> operationCache = CacheBuilder.newBuilder()
                                                                      .expireAfterWrite(1, HOURS)
                                                                      .build();

    public boolean async(final TaskBo task, final TaskOperation operation) {
        return async(task.getId(), operation);
    }

    public boolean async(final Long taskId, final TaskOperation operation) {
        operationCache.put(taskId, operation);
        return true;
    }

    public boolean applied(final TaskBo task, final TaskOperation operation) {
        if (operationCache.getIfPresent(task.getId()) == operation) {
            operationCache.invalidate(task.getId());
            log.info("Task_{}_{} {}", task.getJobId(), task.getId(), operation);
            return true;
        }
        return false;
    }
}
