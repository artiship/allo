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

package io.github.artiship.allo.scheduler.rest.api;

import io.github.artiship.allo.scheduler.core.DependencyScheduler;
import io.github.artiship.allo.scheduler.core.TaskDispatcher;
import io.github.artiship.allo.scheduler.rest.SchedulerBackend;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/tasks")
public class TaskApi {

    @Autowired
    private SchedulerBackend schedulerBackend;

    @Autowired
    private DependencyScheduler dependencyScheduler;

    @Autowired
    private TaskDispatcher taskDispatcher;

    @PutMapping("/{taskId}/rerun")
    public ResponseEntity<Long> rerunTask(@PathVariable Long taskId) {
        return ok(schedulerBackend.rerunTask(taskId));
    }

    @PutMapping("/{taskId}/mark-success")
    public ResponseEntity<Boolean> markSuccessTask(@PathVariable Long taskId) {
        return ok(schedulerBackend.markTaskSuccess(taskId));
    }

    @PutMapping("/{taskId}/mark-fail")
    public ResponseEntity<Boolean> markFailTask(@PathVariable Long taskId) {
        return ok(schedulerBackend.markTaskFail(taskId));
    }

    @PutMapping("/{taskId}/free")
    public ResponseEntity<Void> freeTaskDependencies(@PathVariable Long taskId) {
        schedulerBackend.freeTask(taskId);
        return ok().build();
    }

    @PutMapping("/{taskId}/kill")
    public ResponseEntity<Boolean> killTask(@PathVariable Long taskId) {
        return ok(schedulerBackend.killTask(taskId));
    }

    @GetMapping("/pendings")
    public ResponseEntity<Long> pendingCount() {
        return ok(dependencyScheduler.queuedTaskCount());
    }

    @GetMapping("/waitings")
    public ResponseEntity<Long> waitingCount() {
        return ok(taskDispatcher.queuedTaskCount());
    }
}
