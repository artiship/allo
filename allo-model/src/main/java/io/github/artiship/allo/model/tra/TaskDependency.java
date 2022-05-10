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

package io.github.artiship.allo.model.tra;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

@Data
@Accessors(chain = true)
public class TaskDependency {
    private final static Gson gson = new GsonBuilder().serializeNulls()
                                                      .create();
    private Long jobId;
    private Long taskId;
    private String scheduleTime;
    private String calTimeRange;
    private String taskCron;
    private boolean isReady = false;

    public TaskDependency() {
    }

    public TaskDependency(Long jobId, String scheduleTime, String calTimeRange) {
        this.jobId = jobId;
        this.scheduleTime = scheduleTime;
        this.calTimeRange = calTimeRange;
    }

    public static Set<TaskDependency> parseTaskDependenciesJson(String taskDecenciesJson) {
        if (taskDecenciesJson == null || taskDecenciesJson.length() == 0)
            return emptySet();

        Set<TaskDependency> taskDependencies = gson.fromJson(taskDecenciesJson,
                new TypeToken<Set<TaskDependency>>() {
                }.getType());

        if (taskDependencies != null) return taskDependencies;

        return emptySet();
    }

    public static String toTaskDependenciesJson(Set<TaskDependency> taskDependencies) {
        if (taskDependencies == null)
            return "";

        return gson.toJson(taskDependencies);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskDependency that = (TaskDependency) o;
        return jobId.equals(that.jobId) &&
                calTimeRange.equals(that.calTimeRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, calTimeRange);
    }

    @Override
    public String toString() {
        return "TaskDependency{" +
                "jobId=" + jobId +
                ", taskId=" + taskId +
                ", scheduleTime='" + scheduleTime + '\'' +
                ", calTimeRange='" + calTimeRange + '\'' +
                ", taskCron='" + taskCron + '\'' +
                '}';
    }
}
