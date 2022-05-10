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

public enum TaskTriggerType {
    CRON(1, "by quartz"),
    MANUAL_RUN(2, "run immediately"),
    MANUAL_FREE(21, "skip dependency check"),
    MANUAL_RERUN(22, "rerun"),
    MANUAL_BACK_FILL(23, "back fill"),
    MANUAL_RUN_DOWNSTREAM(24, "run downstream"),
    FAIL_OVER(3, "when worker down"),
    AUTO_RETRY(4, "auto retry when task fail");

    private int code;
    private String desc;

    TaskTriggerType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static TaskTriggerType of(int code) {
        for (TaskTriggerType triggerType : TaskTriggerType.values()) {
            if (triggerType.code == code) {
                return triggerType;
            }
        }
        throw new IllegalArgumentException("unsupported action type " + code);
    }

    public static List<TaskTriggerType> manualTypes() {
        return asList(MANUAL_RUN, MANUAL_FREE, MANUAL_RERUN, MANUAL_BACK_FILL, MANUAL_RUN_DOWNSTREAM);
    }

    public static List<TaskTriggerType> manualTypesExceptSingleCompletement() {
        return asList(MANUAL_RUN, MANUAL_FREE, MANUAL_RERUN, MANUAL_BACK_FILL, MANUAL_RUN_DOWNSTREAM);
    }

    public int getCode() {
        return this.code;
    }
}
