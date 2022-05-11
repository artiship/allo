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

import io.github.artiship.allo.model.enums.WorkerState;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.List;

import static com.google.common.base.Joiner.on;

@Data
@Accessors(chain = true)
public class WorkerBo {
    private Long id;
    private String host;
    private Integer port;
    private WorkerState workerState;
    private List<String> workerGroups;
    private Double cpuUsage;
    private Double memoryUsage;
    private Integer maxTasks;
    private Integer runningTasks;
    private LocalDateTime lastHeartbeatTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public Integer getNodeStateCode() {
        return this.workerState == null ? null : this.workerState.getCode();
    }

    public String getNodeGroupStr() {
        return this.workerGroups == null ? null : on(",").join(workerGroups);
    }

    public Integer getMaxTasks() {
        return this.maxTasks == null ? Integer.valueOf(0) : this.maxTasks;
    }

    public Integer getRunningTasks() {
        return this.runningTasks == null ? Integer.valueOf(0) : this.runningTasks;
    }

    public boolean hasTaskSlots() {
        return (this.getMaxTasks() - this.getRunningTasks()) > 0;
    }

    public Integer availableSlots() {
        return this.getMaxTasks() - this.getRunningTasks();
    }

    public boolean inWorkerGroups(List<String> workerGroups) {
        if (workerGroups == null || workerGroups.isEmpty()) return true;
        if (this.workerGroups == null) return false;
        return workerGroups.stream().anyMatch(workerGroups::contains);
    }

    public boolean notInFailedList(List<String> lastFailedHosts) {
        if (lastFailedHosts == null || lastFailedHosts.isEmpty()) return true;
        if (lastFailedHosts.contains(this.host)) return false;
        return true;
    }

    public String getIpAndPortStr() {
        return this.getHost() + ":" + this.getPort();
    }

    public boolean isActive() {
        return workerState == WorkerState.ACTIVE;
    }
}
