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

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Data
@Accessors(chain = true)
public class JobRelation {
    @Id
    private Long id;
    private Long jobId;
    private Long parentJobId;
    private Integer dependencyType;
    private String dependencyRange;
    private String dependencyRule;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public static JobRelation of(Long jobId, Long parentJobId) {
        return new JobRelation(jobId, parentJobId);
    }

    public JobRelation(Long jobId, Long parentJobId) {
        requireNonNull(parentJobId, "Parent job id is null");
        requireNonNull(jobId, "Child job id is null");

        this.jobId = jobId;
        this.parentJobId = parentJobId;
    }

    public JobRelation() {
    }

    public static List<JobRelation> from(List<JobRelation> schedulerJobRelations) {
        List<JobRelation> collect = schedulerJobRelations.stream().map(e -> new JobRelation()
                .setJobId(e.getJobId())
                .setParentJobId(e.getParentJobId())).collect(Collectors.toList());
        return collect;
    }
}
