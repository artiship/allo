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

import io.github.artiship.allo.tra.parser.DependencyBaseVisitor;
import io.github.artiship.allo.tra.parser.DependencyParser;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

class DependencyRangeOffsetVisitor extends DependencyBaseVisitor<LocalDateTime> {
    private final LocalDateTime baseTime;

    public DependencyRangeOffsetVisitor(LocalDateTime baseTime) {
        this.baseTime = baseTime;
    }

    @Override
    public LocalDateTime visitOffset(DependencyParser.OffsetContext ctx) {
        return baseTime.plus(getOffsetScalar(ctx), getChronosUnit(ctx));
    }

    private Integer getOffsetScalar(DependencyParser.OffsetContext ctx) {
        Integer operand = Integer.valueOf(ctx.N().getText());
        if (ctx.operation() == null) {
            return operand;
        }

        if ("+".equals(ctx.operation().getText())) {
            return operand;
        }

        return -operand;
    }

    private ChronoUnit getChronosUnit(DependencyParser.OffsetContext ctx) {
        ChronoUnit unit = null;
        switch (ctx.timeUnit().getChild(0).getText()) {
            case "y":
                unit = ChronoUnit.YEARS;
                break;
            case "M":
                unit = ChronoUnit.MONTHS;
                break;
            case "d":
                unit = ChronoUnit.DAYS;
                break;
            case "h":
                unit = ChronoUnit.HOURS;
                break;
            case "m":
                unit = ChronoUnit.MINUTES;
                break;
            case "s":
                unit = ChronoUnit.SECONDS;
                break;
        }
        return unit;
    }
}
