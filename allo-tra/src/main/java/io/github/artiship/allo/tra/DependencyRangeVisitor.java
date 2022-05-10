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

import io.github.artiship.allo.model.tra.DependencyRange;
import io.github.artiship.allo.model.tra.DependencyRangeOffset;
import io.github.artiship.allo.tra.parser.DependencyBaseVisitor;
import io.github.artiship.allo.tra.parser.DependencyParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

class DependencyRangeVisitor extends DependencyBaseVisitor<DependencyRange> {
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final LocalDateTime childScheduleTime;

    public DependencyRangeVisitor(LocalDateTime childScheduleTime) {
        this.childScheduleTime = childScheduleTime;
    }

    @Override
    public DependencyRange visitDependencyRange(DependencyParser.DependencyRangeContext ctx) {
        String timeFormat =
                new DependencyRangeFormatVisitor().visitRangeTimeFormat(ctx.rangeTimeFormat());

        LocalDateTime baseTime =
                LocalDateTime.parse(
                        childScheduleTime.format(DateTimeFormatter.ofPattern(timeFormat)),
                        formatter);

        return DependencyRange.of(
                DependencyRangeOffset.of(
                        processOffsets(baseTime, ctx.range().rangeStart().offset()),
                        ctx.bracketStart().getChild(0).getText()),
                DependencyRangeOffset.of(
                        processOffsets(baseTime, ctx.range().rangeEnd().offset()),
                        ctx.bracketEnd().getChild(0).getText()));
    }

    private LocalDateTime processOffsets(
            LocalDateTime baseTime, List<DependencyParser.OffsetContext> offsetContexts) {
        LocalDateTime timeShifted = baseTime.plusNanos(0);

        for (DependencyParser.OffsetContext offsetCxt : offsetContexts) {
            timeShifted = new DependencyRangeOffsetVisitor(timeShifted).visitOffset(offsetCxt);
        }
        return timeShifted;
    }
}
