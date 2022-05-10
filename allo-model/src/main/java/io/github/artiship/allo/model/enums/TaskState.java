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

package io.github.artiship.allo.model.enums;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public enum TaskState {
    PENDING(1, "Pending for parents to accomplish"),
    WAITING(2, "Waiting for available worker to execute"),
    WAITING_PARALLELISM_LIMIT(21, "Task is limited by the job max concurrency"),
    WAITING_TASK_SLOT(22, "Waiting for available worker or task slot"),
    WAITING_COMPLEMENT_LIMIT(23, "10 am ~ 24 pm"),
    DISPATCHED(3, "Dispatched to worker"),
    RUNNING(4, "Executing in a worker node"),
    FAIL_OVER(5, "When worker down"),
    BLOCKED(6, "paused"),
    RETRYING(7, "retrying"),
    KILLED(8, "Killed"),
    SUCCESS(9, "Success"),
    FAIL(10, "Failed"),
    UNKNOWN(-1, "");

    private Integer code;
    private String desc;

    TaskState(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static TaskState of(int code) {
        for (TaskState taskState : TaskState.values()) {
            if (taskState.code == code) {
                return taskState;
            }
        }
        return UNKNOWN;
    }

    public static List<TaskState> finishStates() {
        return asList(KILLED, FAIL_OVER, SUCCESS, FAIL);
    }

    public static List<Integer> finishStateCodes() {
        return finishStates().stream()
                             .map(s -> s.getCode())
                             .collect(toList());
    }

    public static List<TaskState> waitingStates() {
        return asList(WAITING, WAITING_TASK_SLOT, WAITING_PARALLELISM_LIMIT, WAITING_COMPLEMENT_LIMIT);
    }

    public static List<Integer> waitingStateCodes() {
        return waitingStates().stream()
                              .map(s -> s.getCode())
                              .collect(toList());
    }

    public static List<TaskState> unFinishedStates() {
        return stream(TaskState.values()).filter(s -> !finishStates().contains(s))
                                         .collect(toList());
    }

    public static List<Integer> unFinishedStateCodes() {
        return unFinishedStates().stream()
                                 .map(s -> s.getCode())
                                 .collect(toList());
    }

    public int getCode() {
        return this.code;
    }
}
