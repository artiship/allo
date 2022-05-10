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

import lombok.Data;

import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.lang.Integer.valueOf;
import static java.util.Objects.requireNonNull;

@Data
public class ZkWorker {
    private final String ip;
    private final Integer port;

    public static ZkWorker from(String nodeInfoStr) {
        List<String> list = on(":").splitToList(nodeInfoStr);
        return new ZkWorker(list.get(0), valueOf(list.get(1)));
    }

    public ZkWorker(String ip, Integer port) {
        requireNonNull(ip, "Worker ip is null");
        requireNonNull(port, "Worker ip is null");

        this.ip = ip;
        this.port = port;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
