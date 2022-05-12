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

public enum ExecutionMode {
    LOCAL(1),
    KUBERNETES(2);

    private int code;

    ExecutionMode(int code) {
        this.code = code;
    }

    public static ExecutionMode of(int code) {
        for (ExecutionMode executionMode : ExecutionMode.values()) {
            if (executionMode.code == code) {
                return executionMode;
            }
        }
        throw new RuntimeException("Code " + code + " does not match.");
    }


    public int getCode() {
        return this.code;
    }
}