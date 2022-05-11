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

package io.github.artiship.allo.tra;

import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.model.tra.DependencyRange;
import io.github.artiship.allo.quartz.utils.QuartzUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CustomizedDependencyAnalyzer implements DependencyAnalyzer {
    private final String parentCronExpr;
    private final String dependencyExpr;
    private final LocalDateTime childScheduleTime;

    public CustomizedDependencyAnalyzer(
            String parentCronExpr, String dependencyExpr, LocalDateTime childScheduleTime) {
        this.parentCronExpr = parentCronExpr;
        this.dependencyExpr = dependencyExpr;
        this.childScheduleTime = childScheduleTime;
    }

    @Override
    public List<LocalDateTime> parents() {
        DependencyRange range = DependencyExprParser.parseRange(dependencyExpr, childScheduleTime);

        LocalDateTime parentStartTime = range.getRangeStart().getRangeTime();
        LocalDateTime parentEndTime = range.getRangEnd().getRangeTime();

        try {
            return QuartzUtils.computeScheduleTimes(
                            parentCronExpr,
                            TimeUtils.toDate(parentStartTime),
                            TimeUtils.toDate(parentEndTime))
                    .stream()
                    .map(TimeUtils::fromDate)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }
}
