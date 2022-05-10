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

import io.github.artiship.allo.database.entity.Job;
import io.github.artiship.allo.database.entity.JobRelation;
import io.github.artiship.allo.scheduler.dependency.TaskSuccessRecord;
import io.github.artiship.allo.scheduler.rest.SchedulerBackend;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/jobs")
public class JobApi {

    @Autowired
    private SchedulerBackend schedulerBackend;

    @GetMapping("/{jobId}")
    public ResponseEntity<Job> getJob(@PathVariable Long jobId) {
        return ok(schedulerBackend.getJob(jobId));
    }

    @PostMapping("/list")
    public ResponseEntity<List<Job>> getJobs(@RequestBody List<Long> jobIds) {
        return ok(schedulerBackend.getJobs(jobIds));
    }

    @PutMapping("/{jobId}/schedule")
    public ResponseEntity<Void> scheduleJob(@PathVariable Long jobId) {
        schedulerBackend.scheduleJob(jobId);
        return ok().build();
    }

    @PostMapping("/")
    public ResponseEntity<Job> scheduleJob(@RequestBody Job schedulerJob) {
        return ok(schedulerBackend.scheduleJob(schedulerJob));
    }

    @PutMapping("/{jobId}/pause")
    public ResponseEntity<Void> pauseJob(@PathVariable Long jobId) {
        schedulerBackend.pauseJob(jobId);
        return ok().build();
    }

    @PutMapping("/{jobId}/resume")
    public ResponseEntity<Void> resumeJob(@PathVariable Long jobId) {
        schedulerBackend.resumeJob(jobId);
        return ok().build();
    }

    @DeleteMapping("/{jobId}/delete")
    public ResponseEntity<Void> deleteJob(@PathVariable Long jobId) {
        schedulerBackend.deleteJob(jobId);
        return ok().build();
    }

    @PostMapping("/{jobId}/parents")
    public ResponseEntity<Void> addDependency(@PathVariable Long jobId,
                                              @RequestBody List<JobRelation> jobRelations) {

        this.schedulerBackend.addJobDependencies(jobId, jobRelations);
        return ok().build();
    }

    @GetMapping("/{jobId}/parents")
    public ResponseEntity<List<Job>> getParents(@PathVariable Long jobId) {
        return ok(this.schedulerBackend.getParentJobs(jobId));
    }

    @GetMapping("/{jobId}/children")
    public ResponseEntity<List<Job>> getChildren(@PathVariable Long jobId) {
        return ok(schedulerBackend.getChildJobs(jobId));
    }

    @GetMapping("/{jobId}/tasks/running")
    public ResponseEntity<Set<Long>> getRunningTasks(@PathVariable Long jobId) {
        return ok(this.schedulerBackend.getRunningTasks(jobId));
    }

    @GetMapping("/{jobId}/successes")
    public ResponseEntity<Set<TaskSuccessRecord>> getSuccesses(@PathVariable Long jobId) {
        return ok(this.schedulerBackend.getJobSuccesses(jobId));
    }

    @DeleteMapping("/{jobId}/parents/{parentJobId}")
    public ResponseEntity<Void> removeDependency(@PathVariable Long jobId, @PathVariable Long parentJobId) {
        this.schedulerBackend.removeJobDependency(jobId, parentJobId);
        return ok().build();
    }

    @PostMapping("/{jobId}/dependencies/delete")
    public ResponseEntity<Void> removeDependency(@PathVariable Long jobId,
                                                 @RequestBody List<JobRelation> jobRelations) {
        this.schedulerBackend.removeJobDependencies(jobId, jobRelations);
        return ok().build();
    }

    @PutMapping("/{jobId}/run")
    public ResponseEntity<Long> runJob(@PathVariable Long jobId) {
        return ok(schedulerBackend.runJob(jobId));
    }

    @PutMapping("/{jobId}/run/{pointInTime}")
    public ResponseEntity<Long> runJobAtPointInTime(@PathVariable Long jobId,
                                                    @PathVariable(value = "pointInTime")
                                                    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime pointInTime) {
        return ok(schedulerBackend.runJob(jobId, pointInTime));
    }
}
