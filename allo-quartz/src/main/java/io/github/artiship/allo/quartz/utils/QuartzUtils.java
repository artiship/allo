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

package io.github.artiship.allo.quartz.utils;

import io.github.artiship.allo.model.enums.JobCycle;
import io.github.artiship.allo.model.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronExpression;
import org.quartz.TriggerUtils;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

import static com.google.gson.internal.$Gson$Preconditions.checkArgument;
import static io.github.artiship.allo.model.enums.JobCycle.truncateUnit;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static lombok.Lombok.checkNotNull;

@Slf4j
public class QuartzUtils {

    public static boolean isSatisfiedBy(String cron, LocalDateTime scheduleTime) {
        try {
            return new CronExpression(cron).isSatisfiedBy(TimeUtils.toDate(scheduleTime));
        } catch (Exception e) {
            log.info("Check satisfy fail: cron= " + cron + ", time=" + scheduleTime, e);
        }
        return false;
    }

    public static boolean isSatisfiedBy(String cron, Date scheduleTime) throws ParseException {
        return new CronExpression(cron).isSatisfiedBy(scheduleTime);
    }

    public static LocalDateTime preScheduleTimeOfSomeTime(String cron, LocalDateTime someTime) {
        return preScheduleTime(cron, nextScheduleTime(cron, someTime));
    }

    public static LocalDateTime scheduleTime(String cronExpression, LocalDateTime someTime) {
        try {
            return TimeUtils.fromDate(scheduleTime(cronExpression, TimeUtils.toDate(someTime)));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Compute schedule time fail: cron= " + cronExpression + ", time=" + someTime,
                    e);
        }
    }

    public static LocalDateTime nextScheduleTime(
            String cronExpression, LocalDateTime scheduleTime) {
        try {
            return TimeUtils.fromDate(
                    nextScheduleTime(cronExpression, TimeUtils.toDate(scheduleTime)));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Compute next schedule time fail: cron= "
                            + cronExpression
                            + ", time="
                            + scheduleTime,
                    e);
        }
    }

    public static LocalDateTime preScheduleTime(String cronExpression, LocalDateTime scheduleTime) {
        try {
            return TimeUtils.fromDate(
                    preScheduleTime(cronExpression, TimeUtils.toDate(scheduleTime)));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Compute previous schedule time fail: cron= "
                            + cronExpression
                            + ", time="
                            + scheduleTime,
                    e);
        }
    }

    public static Date scheduleTime(String cronExpression, Date someTime) throws Exception {
        CronExpression cron = new CronExpression(cronExpression);
        if (cron.isSatisfiedBy(someTime)) return someTime;

        return cron.getNextValidTimeAfter(someTime);
    }

    public static Date nextScheduleTime(String cronExpression) throws Exception {
        return nextScheduleTime(cronExpression, new Date());
    }

    public static Date nextScheduleTime(String cronExpression, Date date) throws Exception {
        if (cronExpression == null || cronExpression.length() == 0) {
            return null;
        }
        CronExpression cron = new CronExpression(cronExpression);
        Date nextFireDate = cron.getNextValidTimeAfter(date);
        return nextFireDate;
    }

    public static Date preScheduleTime(String cronExpression) throws Exception {
        return preScheduleTime(cronExpression, new Date());
    }

    public static Date preScheduleTime(String cronExpression, Date date) throws Exception {
        int i = 0;
        Date next = date;
        while (true) {
            i++;
            next = nextScheduleTime(cronExpression, next);
            long interval = next.getTime() - date.getTime();
            Date pre = new Date(date.getTime() - interval);
            Date result = preScheduleTime(cronExpression, pre, date);
            if (i > 1000 || result != null) {
                if (result != null) {
                    log.debug(
                            "Cron compute pre success: cron={}, time={}, times={}",
                            cronExpression,
                            date,
                            i);
                    return result;
                } else {
                    log.debug(
                            "Cron compute pre fail: cron={}, time={}, times={}",
                            cronExpression,
                            date,
                            i);
                }
                break;
            }
        }
        return null;
    }

    private static Date preScheduleTime(String cronExpression, Date pre, Date date)
            throws Exception {
        if (pre.after(date)) {
            return null;
        }
        Date next = nextScheduleTime(cronExpression, pre);
        if (next.after(date) || next.equals(date)) {
            return null;
        }

        while (next != null && next.before(date)) {
            pre = next;
            next = preScheduleTime(cronExpression, next, date);
        }
        return pre;
    }

    public static JobCycle jobCycle(String cron) {
        return JobCycle.from(intervalOf(cron));
    }

    public static long intervalOf(String cron) {
        return QuartzUtils.interval(cron);
    }

    public static long interval(CronExpression cron) {
        Date next = cron.getNextValidTimeAfter(new Date());
        Date nextOfNext = cron.getNextValidTimeAfter(next);

        return nextOfNext.getTime() - next.getTime();
    }

    public static long interval(String cronExpression) {
        try {
            return interval(new CronExpression(cronExpression));
        } catch (Exception e) {
            throw new RuntimeException("Get interval fail: cron=" + cronExpression, e);
        }
    }

    public static List<LocalDateTime> computeScheduleTimes(
            String cronExpression, LocalDateTime from, LocalDateTime to) {
        try {
            return TriggerUtils.computeFireTimesBetween(
                            getCronTrigger(cronExpression),
                            null,
                            TimeUtils.toDate(from),
                            TimeUtils.toDate(to))
                    .stream()
                    .map(date -> TimeUtils.fromDate(date))
                    .collect(toList());
        } catch (ParseException e) {
            throw new RuntimeException(
                    "Compute schedule times fail: cron="
                            + cronExpression
                            + ", from="
                            + from
                            + ", to="
                            + to,
                    e);
        }
    }

    public static List<Date> computeScheduleTimes(String cronExpression, Date from, Date to)
            throws Exception {
        return TriggerUtils.computeFireTimesBetween(getCronTrigger(cronExpression), null, from, to);
    }

    public static List<Date> computeScheduleTimes(
            String parentCronExpression, String childCronExpression) throws Exception {
        return computeParentScheduleTimes(parentCronExpression, childCronExpression, new Date());
    }

    public static List<Date> computeParentScheduleTimes(
            String parentCronExpression, String childCronExpression, Date childScheduleTime)
            throws Exception {

        return computeScheduleTimes(
                parentCronExpression,
                preScheduleTime(childCronExpression, childScheduleTime),
                childScheduleTime);
    }

    public static List<LocalDateTime> calTimeRangeNew(LocalDateTime scheduleTime, String cron) {
        long interval = intervalOf(cron);

        ChronoUnit truncateUnit = truncateUnit(interval);

        LocalDateTime endTime = truncateTo(scheduleTime, truncateUnit);
        LocalDateTime startTime =
                truncateTo(QuartzUtils.preScheduleTime(cron, scheduleTime), truncateUnit);

        return asList(startTime, endTime);
    }

    private static CronTriggerImpl getCronTrigger(String cron) throws ParseException {
        CronTriggerImpl cronTrigger = new CronTriggerImpl();
        cronTrigger.setCronExpression(cron);
        return cronTrigger;
    }

    public static LocalDateTime truncateTo(LocalDateTime localDateTime, ChronoUnit unit) {
        try {
            return localDateTime.truncatedTo(unit);
        } catch (UnsupportedTemporalTypeException e) {

        }
        return localDateTime.truncatedTo(ChronoUnit.DAYS);
    }

    public static String calTimeRangeStr(LocalDateTime scheduleTime, String cron) {
        return calTimeRangeStr(calTimeRangeNew(scheduleTime, cron));
    }

    public static String calTimeRangeStr(List<LocalDateTime> range) {
        checkNotNull(range, "Cal time range is null.");
        checkArgument(range.size() == 2);

        return new StringJoiner("~", "(", "]")
                .add(TimeUtils.toStr(range.get(0)))
                .add(TimeUtils.toStr(range.get(1)))
                .toString();
    }
}
