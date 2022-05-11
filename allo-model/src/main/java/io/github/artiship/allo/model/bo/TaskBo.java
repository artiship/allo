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

package io.github.artiship.allo.model.bo;

import com.google.gson.Gson;
import io.github.artiship.allo.model.enums.JobPriority;
import io.github.artiship.allo.model.enums.JobType;
import io.github.artiship.allo.model.enums.TaskState;
import io.github.artiship.allo.model.enums.TaskTriggerType;
import io.github.artiship.allo.model.ha.ZkLostTask;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.*;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.ComparisonChain.start;
import static com.google.common.collect.Ordering.natural;
import static io.github.artiship.allo.model.enums.JobPriority.MEDIUM;
import static io.github.artiship.allo.model.enums.TaskState.*;
import static io.github.artiship.allo.model.enums.TaskTriggerType.CRON;
import static io.github.artiship.allo.model.enums.TaskTriggerType.MANUAL_RUN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.between;
import static java.time.LocalDateTime.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Optional.empty;

@Data
@Accessors(chain = true)
public class TaskBo implements Comparable<TaskBo> {
    private Long id;
    private Long jobId;
    private String taskName;
    private TaskState taskState;
    private String jobStoragePath;
    private List<String> workerGroups;
    private String workerHost;
    private Integer workerPort;
    private Integer retryTimes;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private Long pid;
    private Set<String> applicationIds;
    private TaskTriggerType taskTriggerType;
    private JobType jobType;
    private Boolean isSelfDependent;
    private Boolean isSkipRun;
    private JobPriority jobPriority;
    private String scheduleCron;
    private LocalDateTime scheduleTime; // quartz fire time, includes missing fire time
    private LocalDateTime pendingTime; // time of submit to task scheduler
    private LocalDateTime waitingTime; // time of submit to task dispatcher
    private LocalDateTime dispatchedTime; // time of dispatched to worker
    private LocalDateTime startTime; // time of start running
    private LocalDateTime endTime; // time of success/fail/killed
    private Long elapseTime; // endTime - startTime, duration of task execution
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private Optional<Boolean> isFirstOfJob = empty();
    private Set<Long> skipDependencies = new HashSet<>();
    private Integer penalty;
    private String taskDependenciesJson;

    public static TaskBo from(JobBo job) {
        return new TaskBo()
                .setJobId(job.getId())
                .setTaskName(job.getJobName())
                .setJobStoragePath(job.getJobStoragePath())
                .setWorkerGroups(job.getWorkerGroups())
                .setMaxRetryTimes(job.getMaxRetryTimes())
                .setRetryInterval(job.getRetryInterval())
                .setJobPriority(job.getJobPriority() != null ? job.getJobPriority() : MEDIUM)
                .setJobType(job.getJobType())
                .setIsSelfDependent(job.getIsSelfDependent())
                .setScheduleCron(job.getScheduleCron());
    }

    public static TaskBo from(ZkLostTask zkLostTask) {
        return new TaskBo()
                .setId(zkLostTask.getId())
                .setTaskState(zkLostTask.getState())
                .setEndTime(zkLostTask.getEndTime());
    }

    public TaskBo toRenewTask() {
        return new TaskBo()
                .setJobId(this.jobId)
                .setTaskName(this.taskName)
                .setJobStoragePath(this.jobStoragePath)
                .setWorkerGroups(this.getWorkerGroups())
                .setRetryTimes(this.retryTimes == null ? 0 : this.retryTimes)
                .setRetryInterval(this.retryInterval)
                .setMaxRetryTimes(this.maxRetryTimes)
                .setJobPriority(this.jobPriority)
                .setJobType(this.jobType)
                .setIsSelfDependent(this.isSelfDependent)
                .setIsSkipRun(this.isSkipRun)
                .setScheduleCron(this.scheduleCron)
                .setScheduleTime(this.scheduleTime)
                .setTaskDependenciesJson(this.taskDependenciesJson)
                .setTaskTriggerType(this.taskTriggerType);
    }

    private String workerGroupsToString() {
        return this.getWorkerGroups() == null ? null : on(",").join(this.getWorkerGroups());
    }

    public Integer getJobTypeCode() {
        return this.jobType == null ? null : this.jobType.getCode();
    }

    public String applicationIdsToString() {
        return this.getApplicationIds() == null ? null : on(",").join(this.getApplicationIds());
    }

    public Integer getTaskStateCode() {
        return this.taskState == null ? null : taskState.getCode();
    }

    public Integer getTaskTriggerTypeCode() {
        return this.taskTriggerType == null ? null : taskTriggerType.getCode();
    }

    public Boolean isTriggerManually() {
        return this.taskTriggerType == MANUAL_RUN;
    }

    public boolean isTerminated() {
        if (taskState == FAIL || taskState == SUCCESS || taskState == KILLED) {
            return true;
        }
        return false;
    }

    public boolean retryable() {
        if (this.maxRetryTimes == null || this.maxRetryTimes == 0) return false;

        if ((this.retryTimes == null ? 0 : this.retryTimes) >= maxRetryTimes) return false;

        return true;
    }

    public boolean isSelfDependent() {
        return this.isSelfDependent == null ? false : this.isSelfDependent;
    }

    public TaskBo penalty() {
        if (penalty < 0) {
            penalty = 0;
        }

        this.penalty++;
        return this;
    }

    public static TaskBo from(String jsonStr) {
        return new Gson().fromJson(jsonStr, TaskBo.class);
    }

    public static TaskBo from(byte[] jsonBytes) {
        return from(new String(jsonBytes, UTF_8));
    }

    public Long getElapseTime() {
        if (this.elapseTime != null) return this.elapseTime;

        if (this.startTime != null && this.endTime != null)
            return between(this.startTime, this.endTime).toMillis();

        return null;
    }

    public boolean isFirstOfJob() {
        if (!this.isFirstOfJob.isPresent()) return false;

        return isFirstOfJob.get();
    }

    public void addSkipDependency(Long parentJobId) {
        this.skipDependencies.add(parentJobId);
    }

    public void addSkipDecencies(Set<Long> parentJobIds) {
        this.skipDependencies.addAll(parentJobIds);
    }

    public boolean shouldSkipDependency(Long parentJobId) {
        return this.skipDependencies.contains(parentJobId);
    }

    @Override
    public int compareTo(TaskBo that) {
        return start().compare(this.jobPriority, that.jobPriority, natural().reverse())
                .compare(this.penalty, that.penalty)
                .compare(this.scheduleTime, that.scheduleTime, natural().nullsFirst())
                .result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskBo taskBo = (TaskBo) o;
        return id.equals(taskBo.id)
                && taskBo.scheduleTime
                        .truncatedTo(SECONDS)
                        .equals(scheduleTime == null ? null : scheduleTime.truncatedTo(SECONDS));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, scheduleTime);
    }

    public void incrRetryTimes() {
        if (this.retryTimes == null) this.retryTimes = 1;
        this.retryTimes++;
    }

    public boolean isGarbage() {
        return this.taskTriggerType == CRON && between(this.createTime, now()).toDays() >= 7;
    }

    public boolean isOssPathValid() {
        if (this.jobStoragePath == null || this.jobStoragePath.length() == 0 || !jobStoragePath.contains("/")) {
            return false;
        }
        return true;
    }

    public String traceId() {
        return "Task_" + jobId + "_" + id;
    }

    public boolean triggeredManually() {
        return TaskTriggerType.manualTypes().contains(this.getTaskTriggerType());
    }

    public boolean retryIntervalExceeded() {
        return LocalDateTime.now().isBefore(nextRetryTime());
    }

    public LocalDateTime nextRetryTime() {
        if (endTime == null) {
            return updateTime.plusMinutes(retryInterval);
        }
        return endTime.plusMinutes(retryInterval);
    }

    public void addApplicationId(String applicationId) {
        if (applicationId == null || applicationId.length() == 0) {
            return;
        }

        if (this.applicationIds == null) {
            this.applicationIds = new HashSet<>();
        }

        this.applicationIds.add(applicationId);
    }
}
