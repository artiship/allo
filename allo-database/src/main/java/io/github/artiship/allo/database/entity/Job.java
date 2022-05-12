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

import io.github.artiship.allo.model.bo.JobBo;
import io.github.artiship.allo.model.enums.ExecutionMode;
import io.github.artiship.allo.model.enums.JobPriority;
import io.github.artiship.allo.model.enums.JobType;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.time.LocalDateTime;
import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.time.LocalDateTime.now;
import static java.util.Collections.emptyList;

@Accessors(chain = true)
@Data
public class Job implements Persistable {
    @Id
    private Long id;
    private String jobName;
    private Integer jobType;
    private Integer jobPriority;
    private Integer jobCycle;
    private String scheduleCron;
    private Integer isSelfDependent;
    private Integer isSkipRun;
    private String jobStoragePath;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private String workerGroups;
    private String description;
    private Integer jobState;
    private Integer executionMode;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    @Transient
    private boolean isNewEntity = false;

    @Override
    public boolean isNew(){
        return   isNewEntity;
    }

    public void  setNew(boolean isNew){
        this.isNewEntity = isNew;
    }

    public JobBo toJobBo() {
        return new JobBo().setId(getId())
                .setJobName(getJobName())
                .setJobType(getJobType() == null ? null
                        : JobType.of(getJobType()))
                .setJobPriority(getJobPriority() == null ? null
                        : JobPriority.of(getJobPriority()))
                .setIsSelfDependent(getIsSelfDependentBoolean())
                .setIsSkipRun(getIsSkipRunBoolean())
                .setScheduleCron(getScheduleCron())
                .setMaxRetryTimes(getMaxRetryTimes())
                .setRetryInterval(getRetryInterval())
                .setWorkerGroups(getListOfWorkerGroups())
                .setDescription(getDescription())
                .setExecutionMode(ExecutionMode.of(getExecutionMode()))
                .setJobStoragePath(getJobStoragePath());
    }

    public Boolean getIsSelfDependentBoolean() {
        return (this.isSelfDependent == null || this.isSelfDependent == 0) ? false : true;
    }

    public Boolean getIsSkipRunBoolean() {
        return (this.isSkipRun == null || this.isSkipRun == 0) ? false : true;
    }

    public List<String> getListOfWorkerGroups() {
        if (this.workerGroups == null)
            return emptyList();

        return on(",").splitToList(this.getWorkerGroups());
    }

    public Job updateIgnoreNull(Job schedulerJob) {
        if (schedulerJob.getId() != null) this.id = schedulerJob.getId();
        if (schedulerJob.getJobName() != null) this.jobName = schedulerJob.getJobName();
        if (schedulerJob.getJobType() != null) this.jobType = schedulerJob.getJobType();
        if (schedulerJob.getJobPriority() != null) this.jobPriority = schedulerJob.getJobPriority();
        if (schedulerJob.getJobCycle() != null) this.jobCycle = schedulerJob.getJobCycle();
        if (schedulerJob.getIsSkipRun() != null) this.isSkipRun = schedulerJob.getIsSkipRun();
        if (schedulerJob.getScheduleCron() != null) this.scheduleCron = schedulerJob.getScheduleCron();
        if (schedulerJob.getIsSelfDependent() != null) this.isSelfDependent = schedulerJob.getIsSelfDependent();
        if (schedulerJob.getMaxRetryTimes() != null) this.maxRetryTimes = schedulerJob.getMaxRetryTimes();
        if (schedulerJob.getRetryInterval() != null) this.retryInterval = schedulerJob.getRetryInterval();
        if (schedulerJob.getWorkerGroups() != null) this.workerGroups = schedulerJob.getWorkerGroups();
        if (schedulerJob.getJobState() != null) this.jobState = schedulerJob.getJobState();
        if (schedulerJob.getExecutionMode() != null) this.executionMode = schedulerJob.getExecutionMode();
        if (schedulerJob.getDescription() != null) this.description = schedulerJob.getDescription();
        if (schedulerJob.getJobStoragePath() != null) this.jobStoragePath = schedulerJob.getJobStoragePath();
        if (schedulerJob.getCreateTime() != null) this.createTime = schedulerJob.getCreateTime();
        if (schedulerJob.getUpdateTime() != null) this.updateTime = schedulerJob.getUpdateTime();
        this.updateTime = now();
        return this;
    }
}