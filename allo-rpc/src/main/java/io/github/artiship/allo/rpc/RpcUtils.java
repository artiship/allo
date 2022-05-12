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

package io.github.artiship.allo.rpc;

import com.google.common.collect.Lists;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.enums.ExecutionMode;
import io.github.artiship.allo.model.enums.JobType;
import io.github.artiship.allo.model.enums.TaskState;
import io.github.artiship.allo.rpc.api.RpcHeartbeat;
import io.github.artiship.allo.rpc.api.RpcTask;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.artiship.allo.rpc.api.RpcTask.newBuilder;

public class RpcUtils {
    private static final long MILLIS_PER_SECOND = 1000;

    public static RpcTask toRpcTask(TaskBo taskBo) {
        RpcTask.Builder builder = newBuilder();
        if (taskBo.getWorkerHost() != null) builder.setWorkerHost(taskBo.getWorkerHost());
        if (taskBo.getId() != null) builder.setId(taskBo.getId());
        if (taskBo.getId() != null) builder.setJobId(taskBo.getJobId());
        if (taskBo.getRetryInterval() != null) builder.setRetryInterval(taskBo.getRetryInterval());
        if (taskBo.getScheduleTime() != null) {
            builder.setScheduleTime(toProtoTimestamp(taskBo.getScheduleTime()));
        }
        if (taskBo.getStartTime() != null)
            builder.setStartTime(toProtoTimestamp(taskBo.getStartTime()));
        if (taskBo.getApplicationIds() != null) {
            taskBo.getApplicationIds().forEach(appId -> builder.addApplicationId(appId));
        }
        if (taskBo.getEndTime() != null) builder.setEndTime(toProtoTimestamp(taskBo.getEndTime()));
        if (taskBo.getRetryTimes() != null) builder.setRetryTimes(taskBo.getRetryTimes());
        if (taskBo.getMaxRetryTimes() != null) builder.setMaxRetryTimes(taskBo.getMaxRetryTimes());
        if (taskBo.getTaskState() != null) builder.setState(taskBo.getTaskState().getCode());
        if (taskBo.getWorkerPort() != null) builder.setWorkerPort(taskBo.getWorkerPort());
        if (taskBo.getJobType() != null) builder.setJobType(taskBo.getJobType().getCode());
        if (taskBo.getJobStoragePath() != null) builder.setOssPath(taskBo.getJobStoragePath());
        if (taskBo.getExecutionMode() != null)
            builder.setExecutionMode(
                    taskBo.getExecutionMode() == null ? null : taskBo.getExecutionMode().getCode());
        return builder.build();
    }

    public static TaskBo toTaskBo(RpcTask task) {
        LocalDateTime startTime =
                task.getStartTime() == null ? null : toLocalDateTime(task.getStartTime());
        LocalDateTime endTime =
                task.getEndTime() == null ? null : toLocalDateTime(task.getEndTime());
        return new TaskBo()
                .setId(task.getId())
                .setJobId(task.getJobId())
                .setTaskState(TaskState.of(task.getState()))
                .setJobStoragePath(task.getOssPath())
                .setWorkerHost(task.getWorkerHost())
                .setWorkerPort(task.getWorkerPort())
                .setJobType(JobType.of(task.getJobType()))
                .setMaxRetryTimes(task.getMaxRetryTimes())
                .setRetryInterval(task.getRetryInterval())
                .setRetryTimes(task.getRetryTimes())
                .setPid(task.getPid())
                .setApplicationIds(toApplicationList(task.getApplicationIdList()))
                .setExecutionMode(ExecutionMode.of(task.getExecutionMode()))
                .setScheduleTime(toLocalDateTime(task.getScheduleTime()))
                .setStartTime(startTime.getYear() == 1970 ? null : startTime)
                .setEndTime(endTime.getYear() == 1970 ? null : endTime);
    }

    public static Timestamp toProtoTimestamp(LocalDateTime localDateTime) {
        if (localDateTime == null) return null;

        return toProtoTimestamp(Date.from(TimeUtils.instant(localDateTime)));
    }

    public static Timestamp toProtoTimestamp(Date date) {
        return Timestamps.fromMillis(date.getTime());
    }

    public static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return TimeUtils.fromDate(new Date(timestamp.getSeconds() * MILLIS_PER_SECOND));
    }

    private static Set<String> toApplicationList(ProtocolStringList list) {
        return list == null
                ? null
                : list.stream()
                        .filter(i -> i != null && i.trim().length() > 0)
                        .map(i -> i.trim())
                        .collect(Collectors.toSet());
    }

    public static WorkerBo toWorkerBo(RpcHeartbeat heartbeat) {
        return new WorkerBo()
                .setPort(heartbeat.getPort())
                .setHost(heartbeat.getHost())
                .setWorkerGroups(
                        heartbeat.getWorkerGroupsList() == null
                                ? null
                                : Lists.newArrayList(heartbeat.getWorkerGroupsList()))
                .setMaxTasks(heartbeat.getMaxTask())
                .setMemoryUsage(heartbeat.getMemoryUsage())
                .setCpuUsage(heartbeat.getCpuUsage())
                .setLastHeartbeatTime(toLocalDateTime(heartbeat.getTime()))
                .setRunningTasks(heartbeat.getRunningTasks());
    }
}
