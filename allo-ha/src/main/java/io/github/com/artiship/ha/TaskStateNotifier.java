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

package io.github.com.artiship.ha;

import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskState;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class TaskStateNotifier {
    private final List<TaskStateListener> listeners = new LinkedList<>();

    public void registerListener(TaskStateListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    public void notifyRunning(TaskBo task) {
        task.setTaskState(TaskState.RUNNING);

        if (task.getStartTime() == null) {
            task.setStartTime(LocalDateTime.now());
        }

        synchronized (listeners) {
            listeners.forEach(l -> l.onRunning(task));
        }
    }

    public void notifyKilled(TaskBo task) {
        task.setTaskState(TaskState.KILLED).setEndTime(LocalDateTime.now());

        synchronized (listeners) {
            listeners.forEach(l -> l.onKilled(task));
        }
    }

    public void notifySuccess(TaskBo task) {
        task.setTaskState(TaskState.SUCCESS).setEndTime(LocalDateTime.now());

        synchronized (listeners) {
            listeners.forEach(l -> l.onSuccess(task));
        }
    }

    public void notifyFail(TaskBo task) {
        task.setTaskState(TaskState.FAIL).setEndTime(LocalDateTime.now());

        synchronized (listeners) {
            listeners.forEach(l -> l.onFail(task));
        }
    }

    public void notifyFailOver(TaskBo task) {
        task.setTaskState(TaskState.FAIL_OVER);

        synchronized (listeners) {
            listeners.forEach(l -> l.onFailOver(task));
        }
    }
}
