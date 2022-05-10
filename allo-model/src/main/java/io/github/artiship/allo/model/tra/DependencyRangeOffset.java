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

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DependencyRangeOffset {
    private LocalDateTime rangeTime;
    private Boolean inclusive;

    private DependencyRangeOffset(LocalDateTime rangeTime, Boolean inclusive) {
        this.rangeTime = rangeTime;
        this.inclusive = inclusive;
    }

    public static DependencyRangeOffset of(LocalDateTime rangeTime, Boolean inclusive) {
        return new DependencyRangeOffset(rangeTime, inclusive);
    }

    public static DependencyRangeOffset of(LocalDateTime rangeTime, String bracket) {
        return new DependencyRangeOffset(rangeTime, isInclusive(bracket));
    }

    private static boolean isInclusive(String bracket) {
        if ("(".equals(bracket) || ")".equals(bracket)) {
            return true;
        } else if ("[".equals(bracket) || "]".equals(bracket)) {
            return false;
        }
        return false;
    }
}
