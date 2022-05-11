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

package io.github.artiship.allo.scheduler.dependency;

import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.quartz.utils.QuartzUtils;
import io.github.artiship.allo.scheduler.core.TaskDependency;
import lombok.Data;

import java.time.LocalDateTime;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

@Data
public class TaskSuccessRecord implements Comparable<TaskSuccessRecord> {
    private final Long taskId;
    private final LocalDateTime scheduleTime;
    private final String calTimeRange;
    private final String taskCron;
    private final Long executionCost;

    public static TaskSuccessRecord of(Long taskId,
                                       String taskCron,
                                       LocalDateTime scheduleTime,
                                       Long executionCost) {
        requireNonNull(taskCron, "Cron is null");
        requireNonNull(scheduleTime, "Schedule time is null");

        return new TaskSuccessRecord(taskId, taskCron, scheduleTime, executionCost);
    }

    private TaskSuccessRecord(Long taskId,
                              String taskCron,
                              LocalDateTime scheduleTime,
                              Long executionCost) {
        this.taskId = taskId;
        this.taskCron = taskCron;
        this.scheduleTime = scheduleTime;
        this.executionCost = executionCost;
        this.calTimeRange = QuartzUtils.calTimeRangeStr(scheduleTime, taskCron);
    }

    @Override
    public int compareTo(TaskSuccessRecord lastRecord) {
        return scheduleTime.truncatedTo(SECONDS)
                           .compareTo(lastRecord.getScheduleTime()
                                                .truncatedTo(SECONDS));
    }

    public boolean scheduleTimeEquals(LocalDateTime scheduleTime) {
        return this.scheduleTime.truncatedTo(SECONDS)
                                .isEqual((scheduleTime.truncatedTo(SECONDS)));
    }

    public boolean cronEquals(String cron) {
        return this.taskCron.equals(cron);
    }

    public String scheduleTimeStr() {
        return TimeUtils.toStr(this.scheduleTime);
    }

    public String getCalTimeRangeStr() {
        return this.calTimeRange;
    }

    public TaskDependency toTaskDependency() {
        return new TaskDependency().setTaskId(this.taskId)
                                   .setCalTimeRange(this.calTimeRange)
                                   .setScheduleTime(this.scheduleTimeStr())
                                   .setTaskCron(this.taskCron);
    }
}
