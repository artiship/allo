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

import io.github.artiship.allo.model.enums.*;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Accessors(chain = true)
public class JobBo {
    private Long id;
    private String jobName;
    private JobType jobType;
    private JobPriority jobPriority;
    private JobCycle jobCycle;
    private String scheduleCron;
    private Boolean isSelfDependent;
    private Boolean isSkipRun;
    private String jobStoragePath;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private List<String> workerGroups;
    private String description;
    private JobState jobState;
    private ExecutionMode executionMode;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    private List<JobBo> dependencies = new ArrayList<>();

    public JobBo addDependency(JobBo jobBo) {
        dependencies.add(jobBo);
        return this;
    }

    public JobBo addDependencies(List<JobBo> dependencies) {
        if (dependencies == null || dependencies.size() == 0)
            return this;

        this.dependencies.addAll(dependencies);

        return this;
    }
}
