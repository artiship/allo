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

import io.github.artiship.allo.model.bo.WorkerBo;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;

import static java.time.LocalDateTime.now;

@Data
@Accessors(chain = true)
public class Worker {
    @Id
    private Long id;
    private String host;
    private Integer port;
    private Integer workerState;
    private String workerGroups;
    private Double cpuUsage;
    private Double memoryUsage;
    private Integer maxTasks;
    private Integer runningTasks;
    private LocalDateTime lastHeartbeatTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public static Worker from(WorkerBo worker) {
        return null;
    }

    public Worker updateNotNull(Worker worker) {
        if (worker.getId() != null) this.id = worker.getId();
        if (worker.getHost() != null) this.host = worker.getHost();
        if (worker.getPort() != null) this.port = worker.getPort();
        if (worker.getWorkerGroups() != null) this.workerGroups = worker.getWorkerGroups();
        if (worker.getCpuUsage() != null) this.cpuUsage = worker.getCpuUsage();
        if (worker.getMemoryUsage() != null) this.memoryUsage = worker.getMemoryUsage();
        if (worker.getWorkerState() != null) this.workerState = worker.getWorkerState();
        if (worker.getMaxTasks() != null) this.maxTasks = worker.getMaxTasks();
        if (worker.getRunningTasks() != null) this.runningTasks = worker.getRunningTasks();
        if (worker.getLastHeartbeatTime() != null) this.lastHeartbeatTime = worker.getLastHeartbeatTime();
        if (worker.getCreateTime() != null) this.createTime = worker.getCreateTime();
        this.updateTime = now();
        return this;
    }

    public WorkerBo toWorkerBo() {
        return null;
    }
}
