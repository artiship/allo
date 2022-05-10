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

package io.github.artiship.allo.model.ha;

import com.google.gson.Gson;
import io.github.artiship.allo.model.enums.TaskState;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Data
@Accessors(chain = true)
public class ZkLostTask {
    private Long id;
    private TaskState state;
    private LocalDateTime endTime;

    private static Gson gson = new Gson();

    public static ZkLostTask of(Long id, TaskState state, LocalDateTime endTime) {
        return new ZkLostTask(id, state, endTime);
    }

    public ZkLostTask(Long id, TaskState state, LocalDateTime endTime) {
        requireNonNull(id, "Task id is null");
        requireNonNull(state, "Task state is null");
        requireNonNull(endTime, "Task end time is null");

        this.id = id;
        this.state = state;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public byte[] toJsonBytes() {
        return this.toString().getBytes(UTF_8);
    }

    public static ZkLostTask from(String jsonStr) {
        return gson.fromJson(jsonStr, ZkLostTask.class);
    }

    public static ZkLostTask from(byte[] bytes) {
        return from(new String(bytes, UTF_8));
    }
}
