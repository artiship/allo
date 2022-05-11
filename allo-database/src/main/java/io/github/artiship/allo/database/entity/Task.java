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

package io.github.artiship.allo.database.entity;

import com.google.common.base.Splitter;
import io.github.artiship.allo.model.bo.TaskBo;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static java.time.LocalDateTime.now;


@Data
@Accessors(chain = true)
public class Task {
    @Id
    private Long id;
    private Long jobId;
    private String taskName;
    private Integer taskState;
    private String jobStoragePath;
    private String workerGroups;
    private String workerHost;
    private Integer workerPort;
    private Integer retryTimes;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private Long pid;
    private String applicationIds;
    private Integer taskTriggerType;
    private Integer jobType;
    private Integer isSelfDependent;
    private Integer jobPriority;
    private String scheduleCron;
    private LocalDateTime scheduleTime; //quartz fire time, includes missing fire time
    private LocalDateTime pendingTime; //time of submit to task scheduler
    private LocalDateTime waitingTime; //time of submit to task dispatcher
    private LocalDateTime dispatchedTime; //time of dispatched to worker
    private LocalDateTime startTime; //time of start running
    private LocalDateTime endTime;  //time of success/fail/killed
    private Long elapseTime;    // endTime - startTime, duration of task execution
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public static Task from(TaskBo taskBo) {
        return null;
    }

    public Task updateIgnoreNull(Task task) {
        if (task.getId() != null) this.id = task.getId();
        if (task.getJobId() != null) this.jobId = task.getJobId();
        if (task.getTaskName() != null) this.taskName = task.getTaskName();
        if (task.getTaskState() != null) this.taskState = task.getTaskState();
        if (task.getJobStoragePath() != null) this.jobStoragePath = task.getJobStoragePath();
        if (task.getWorkerGroups() != null) this.workerGroups = task.getWorkerGroups();
        if (task.getWorkerHost() != null) this.workerHost = task.getWorkerHost();
        if (task.getWorkerPort() != null) this.workerPort = task.getWorkerPort();
        if (task.getRetryTimes() != null) this.retryTimes = task.getRetryTimes();
        if (task.getMaxRetryTimes() != null) this.maxRetryTimes = task.getMaxRetryTimes();
        if (task.getRetryInterval() != null) this.retryInterval = task.getRetryInterval();
        if (task.getPid() != null) this.pid = task.getPid();
        if (task.getApplicationIds() != null) this.applicationIds = task.getApplicationIds();
        if (task.getElapseTime() != null) this.elapseTime = task.getElapseTime();
        if (task.getTaskTriggerType() != null) this.taskTriggerType = task.getTaskTriggerType();
        if (task.getJobType() != null) this.jobType = task.getJobType();
        if (task.getIsSelfDependent() != null) this.isSelfDependent = task.getIsSelfDependent();
        if (task.getJobPriority() != null) this.jobPriority = task.getJobPriority();
        if (task.getScheduleCron() != null) this.scheduleCron = task.getScheduleCron();
        if (task.getScheduleTime() != null) this.scheduleTime = task.getScheduleTime();
        if (task.getPendingTime() != null) this.pendingTime = task.getPendingTime();
        if (task.getWaitingTime() != null) this.waitingTime = task.getWaitingTime();
        if (task.getDispatchedTime() != null) this.dispatchedTime = task.getDispatchedTime();
        if (task.getStartTime() != null) this.startTime = task.getStartTime();
        if (task.getEndTime() != null) this.endTime = task.getEndTime();
        if (task.getCreateTime() != null) this.createTime = task.getCreateTime();
        this.updateTime = now();

        return this;
    }

    public List<String> getListOfWorkerGroups() {
        if (this.getWorkerGroups() == null || this.getWorkerGroups().length() == 0)
            return Collections.emptyList();

        return Splitter.on(",").splitToList(this.getWorkerGroups());
    }

    public String getStartTimeStr() {
        return this.startTime == null ? "" : this.startTime.toString();
    }

    public String getEndTimeStr() {
        return this.endTime == null ? "" : this.endTime.toString();
    }

    public TaskBo toTaskBo() {
        return null;
    }
}
