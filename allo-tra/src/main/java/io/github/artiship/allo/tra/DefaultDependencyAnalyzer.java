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

import io.github.artiship.allo.model.enums.JobCycle;
import io.github.artiship.allo.model.utils.TimeUtils;
import io.github.artiship.allo.quartz.utils.QuartzUtils;
import io.github.artiship.allo.tra.exception.CronNotSatisfiedException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronExpression;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Objects.requireNonNull;
import static org.quartz.TriggerUtils.computeFireTimesBetween;

@Slf4j
public class DefaultDependencyAnalyzer implements DependencyAnalyzer {

    private final String parentCronExpression;
    private final String childCronExpression;
    private final LocalDateTime childScheduleTime;
    private final boolean isParentSelfDepend;

    private DefaultDependencyAnalyzer(Builder builder) {
        this.parentCronExpression = builder.getParentCronExpression();
        this.childCronExpression = builder.getChildCronExpression();
        this.childScheduleTime = builder.getChildScheduleTime();
        this.isParentSelfDepend = builder.isParentSelfDepend();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public List<LocalDateTime> parents() {
        requireNonNull(parentCronExpression, "Parent cron expression is null.");
        requireNonNull(childCronExpression, "Child cron expression is null.");
        requireNonNull(childScheduleTime, "Child schedule time is null.");

        if (!QuartzUtils.isSatisfiedBy(childCronExpression, childScheduleTime)) {
            throw new CronNotSatisfiedException(
                    "Cron " + childCronExpression + " is not satisfied by " + childScheduleTime);
        }

        try {
            List<LocalDateTime> scheduleTimes =
                    compute().stream().map(TimeUtils::fromDate).collect(Collectors.toList());
            if (scheduleTimes.isEmpty()) {
                return scheduleTimes;
            }

            if (isParentSelfDepend) {
                return Collections.singletonList(scheduleTimes.get(scheduleTimes.size() - 1));
            }

            return scheduleTimes;
        } catch (Exception e) {
            log.warn("Compute {} schedule times fail.", this, e);
            return Collections.emptyList();
        }
    }

    private List<Date> compute() throws Exception {
        List<Date> result = new ArrayList<>();

        CronExpression parentCron = new CronExpression(parentCronExpression);
        CronExpression childCron = new CronExpression(childCronExpression);

        long parentInterval = QuartzUtils.interval(parentCron);
        long childInterval = QuartzUtils.interval(childCron);

        long theBiggerInterval = Math.max(parentInterval, childInterval);

        ChronoUnit truncateUnit = JobCycle.truncateUnit(theBiggerInterval);
        int cycles = JobCycle.numberOfCycles(theBiggerInterval);

        Date baseTime;

        LocalDateTime truncated;
        try {
            truncated = childScheduleTime.truncatedTo(truncateUnit).minus(cycles - 1, truncateUnit);
        } catch (UnsupportedTemporalTypeException e) {
            if (childInterval < parentInterval) {
                truncated =
                        QuartzUtils.preScheduleTime(parentCronExpression, childScheduleTime)
                                .truncatedTo(DAYS)
                                .minus(cycles - 1, DAYS);
            } else if (childInterval == parentInterval) {
                truncated = childScheduleTime.truncatedTo(DAYS).minus(cycles - 1, truncateUnit);
            } else {
                truncated =
                        QuartzUtils.preScheduleTime(childCronExpression, childScheduleTime)
                                .truncatedTo(DAYS)
                                .minus(cycles - 1, DAYS);
            }
        }

        if (childScheduleTime.equals(truncated)) {
            baseTime = TimeUtils.toDate(childScheduleTime);
        } else {
            baseTime = TimeUtils.toDate(truncated);
        }

        Date parentTime = nextValid(parentCron, baseTime);
        Date preParentTime = QuartzUtils.preScheduleTime(parentCronExpression, parentTime);

        if (childInterval <= parentInterval) {
            if (parentCron.isSatisfiedBy(baseTime)) {
                result.add(baseTime);
                return result;
            }

            if (parentTime.getTime() - preParentTime.getTime() == parentInterval) {
                result.add(parentTime);
                return result;
            }

            Date childTime = TimeUtils.toDate(childScheduleTime);
            Date preChildTime = QuartzUtils.preScheduleTime(childCronExpression, childTime);
            Date nextChildTime = QuartzUtils.nextScheduleTime(childCronExpression, childTime);

            long parentDistance = parentTime.getTime() - preParentTime.getTime();
            long childDistance = childTime.getTime() - preChildTime.getTime();

            if (childDistance == parentDistance
                    && parentTime.after(baseTime)
                    && parentTime.before(nextChildTime)) {
                result.add(parentTime);
                return result;
            }

            result.add(preParentTime);
            return result;
        }

        CronTriggerImpl cronTrigger = new CronTriggerImpl();
        cronTrigger.setCronExpression(parentCronExpression);

        Date baseDateTimePre = new Date(baseTime.getTime() - theBiggerInterval);
        Date parentRelativeBasePre = nextValid(parentCron, baseDateTimePre);

        if (parentTime.getTime() - preParentTime.getTime() != parentInterval) {
            if (parentRelativeBasePre.after(preParentTime)) {
                result.add(preParentTime);
                return result;
            }
            return computeFireTimesBetween(cronTrigger, null, parentRelativeBasePre, preParentTime);
        }

        Date nextParentStart =
                QuartzUtils.nextScheduleTime(parentCronExpression, parentRelativeBasePre);

        return computeFireTimesBetween(cronTrigger, null, nextParentStart, parentTime);
    }

    private Date nextValid(CronExpression cron, Date baseTime) {
        if (cron.isSatisfiedBy(baseTime)) {
            return baseTime;
        }
        return cron.getNextValidTimeAfter(baseTime);
    }

    @Override
    public String toString() {
        return "DependencyBuilder{"
                + "parentCronExpression='"
                + parentCronExpression
                + '\''
                + ", childCronExpression='"
                + childCronExpression
                + '\''
                + ", childScheduleTime="
                + childScheduleTime
                + ", isParentSelfDepend="
                + isParentSelfDepend
                + '}';
    }

    @Getter
    public static class Builder {
        private String parentCronExpression;
        private String childCronExpression;
        private LocalDateTime childScheduleTime;
        private boolean isParentSelfDepend = false;

        public Builder parentCronExpression(String parentCronExpression) {
            this.parentCronExpression = parentCronExpression;
            return this;
        }

        public Builder childCronExpression(String childCronExpression) {
            this.childCronExpression = childCronExpression;
            return this;
        }

        public Builder childScheduleTime(LocalDateTime childScheduleTime) {
            this.childScheduleTime = childScheduleTime;
            return this;
        }

        public Builder isParentSelfDepend(boolean isParentSelfDepend) {
            this.isParentSelfDepend = isParentSelfDepend;
            return this;
        }

        public DefaultDependencyAnalyzer build() {
            return new DefaultDependencyAnalyzer(this);
        }
    }
}
