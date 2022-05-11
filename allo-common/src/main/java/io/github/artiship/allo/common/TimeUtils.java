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

package io.github.artiship.allo.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static java.time.LocalDateTime.parse;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Date.from;

public class TimeUtils {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    public static LocalDateTime fromDate(Date date) {
        return date.toInstant().atZone(systemDefault()).toLocalDateTime();
    }

    public static LocalDateTime fromStr(String dateTimeStr) {
        return parse(dateTimeStr, ofPattern(DATE_TIME_FORMAT));
    }

    public static String toStr(LocalDateTime localDateTime) {
        return localDateTime.format(ofPattern(DATE_TIME_FORMAT));
    }

    public static String toStr(Date date) {
        return toStr(fromDate(date));
    }

    public static String format(LocalDateTime localDateTime) {
        return localDateTime.format(ofPattern(DATE_FORMAT));
    }

    public static Date toDate(LocalDateTime localDateTime) {
        return from(instant(localDateTime));
    }

    public static Instant instant(LocalDateTime localDateTime) {
        return localDateTime.atZone(systemDefault()).toInstant();
    }

    public static String getNow() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
