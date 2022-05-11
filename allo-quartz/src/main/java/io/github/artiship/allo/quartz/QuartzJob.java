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

package io.github.artiship.allo.quartz;

import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.model.bo.JobBo;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskTriggerType;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;

@Slf4j
public class QuartzJob implements Job {
    public static final String DATA_KEY = "data";
    private static QuartzListener quartzListener;

    public static void register(QuartzListener quartzListener) {
        QuartzJob.quartzListener = quartzListener;
    }

    public static JobBo getJob(JobDetail jobDetail) {
        return (JobBo) jobDetail.getJobDataMap().get(DATA_KEY);
    }

    @Override
    public void execute(JobExecutionContext context) {
        quartzListener.onTrigger(
                TaskBo.from(getJob(context.getJobDetail()))
                        .setTaskTriggerType(TaskTriggerType.CRON)
                        .setScheduleTime(TimeUtils.fromDate(context.getScheduledFireTime())));
    }
}
