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

import io.github.artiship.allo.model.Service;
import io.github.artiship.allo.model.bo.JobBo;
import io.github.artiship.allo.model.exception.AlloRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import static java.util.Objects.requireNonNull;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerKey.triggerKey;

@Slf4j
public class QuartzSchedulerService implements Service {
    public static final String DATA_KEY = "data";
    public static final String DEFAULT_TRIGGER_GROUP = "allo";
    public static final String DEFAULT_JOB_GROUP = "allo";

    private Scheduler scheduler;

    public QuartzSchedulerService(QuartzConfig quartzConfig) {
        try {
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(quartzConfig);
            scheduler = schedulerFactory.getScheduler();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    public void start() throws Exception {
        scheduler.start();
    }

    public void scheduleJob(JobBo job) {
        scheduleJob(job, QuartzJob.class, false);
    }

    public void scheduleJobWithCompleteMissingPoints(JobBo job, Boolean completeMissingPoints) {
        scheduleJob(job, QuartzJob.class, completeMissingPoints);
    }

    public void scheduleJob(JobBo job, Class<? extends Job> jobClass) {
        this.scheduleJob(job, jobClass, false);
    }

    public void scheduleJob(
            JobBo job, Class<? extends Job> jobClass, Boolean completeMissingPoints) {
        try {
            if (completeMissingPoints == null) completeMissingPoints = false;
            final TriggerKey triggerKey = getTriggerKey(job.getId());
            final CronTrigger cronTrigger = getCronTrigger(job.getId(), job.getScheduleCron());
            if (scheduler.checkExists(triggerKey)) {
                scheduler.addJob(getJobDetail(job.getId(), jobClass), true);
                log.info("Job_{} cron {} added", job.getId(), job.getScheduleCron());

                String oldCronExpression =
                        ((CronTrigger) scheduler.getTrigger(triggerKey)).getCronExpression();
                String newCronExpression = cronTrigger.getCronExpression();

                if (completeMissingPoints
                        && newCronExpression.equalsIgnoreCase(oldCronExpression)) {
                    scheduler.resumeJob(getJobKey(job.getId()));
                    log.info("Job_{} cron {} resumed", job.getId(), job.getScheduleCron());
                    return;
                }

                scheduler.rescheduleJob(
                        triggerKey, getCronTrigger(job.getId(), job.getScheduleCron()));
                log.info("Job_{} cron {} rescheduled", job.getId(), job.getScheduleCron());
                return;
            }

            scheduler.scheduleJob(
                    getJobDetail(job.getId(), jobClass),
                    getCronTrigger(job.getId(), job.getScheduleCron()));
            log.info("Job_{} cron {} scheduled", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            log.error("Job_{} cron {} scheduleJob fail", job.getId(), job.getScheduleCron(), e);
            throw new AlloRuntimeException(e);
        }
    }

    public void pauseJob(JobBo job) {
        requireNonNull(job, "Job is null");

        try {
            JobKey jobKey = getJobKey(job.getId());
            TriggerKey triggerKey = getTriggerKey(job.getId());
            if (scheduler.checkExists(triggerKey)) {
                scheduler.pauseJob(jobKey);
            }

            log.info("Job_{} cron {} paused", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new AlloRuntimeException(e);
        }
    }

    public void resumeJob(JobBo job) {
        requireNonNull(job, "Job is null");

        try {
            JobKey jobKey = getJobKey(job.getId());
            TriggerKey triggerKey = getTriggerKey(job.getId());
            if (scheduler.checkExists(triggerKey)) {
                scheduler.resumeJob(jobKey);
            }
            log.info("Job_{} cron {} resumed", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new AlloRuntimeException(e);
        }
    }

    public void removeJob(JobBo job) {
        requireNonNull(job, "Job is null");

        try {
            TriggerKey triggerKey = getTriggerKey(job.getId());
            JobKey jobKey = getJobKey(job.getId());
            if (scheduler.checkExists(jobKey)) {
                scheduler.pauseTrigger(triggerKey);
                scheduler.unscheduleJob(triggerKey);
                scheduler.deleteJob(jobKey);
            }
            log.info("Job_{} cron {} removed", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new AlloRuntimeException(e);
        }
    }

    public boolean existsJob(JobBo job) {
        try {
            return scheduler != null && scheduler.checkExists(getJobKey(job.getId()));
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean existsTrigger(JobBo job) {
        try {
            return scheduler != null && scheduler.checkExists(getTriggerKey(job.getId()));
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isStarted() {
        try {
            return scheduler != null && scheduler.isStarted();
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isShutdown() {
        try {
            return scheduler != null && scheduler.isShutdown();
        } catch (SchedulerException e) {
            log.error("Shutdown fail", e);
        }
        return false;
    }

    public Scheduler getScheduler() {
        return this.scheduler;
    }

    public void stop() throws Exception {
        if (scheduler == null || scheduler.isShutdown()) return;
        scheduler.shutdown();

        log.info("Quartz scheduler stopped.");
    }

    public TriggerKey getTriggerKey(Long jobId) {
        return triggerKey(quartzKey(jobId), DEFAULT_TRIGGER_GROUP);
    }

    public JobKey getJobKey(Long jobId) {
        return jobKey(quartzKey(jobId), DEFAULT_TRIGGER_GROUP);
    }

    public String quartzKey(Long jobId) {
        return DEFAULT_JOB_GROUP + "_" + jobId;
    }

    public JobDataMap getJobDataMap() {
        final JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(DATA_KEY, this);
        return jobDataMap;
    }

    public JobDetail getJobDetail(Long jobId, Class<? extends Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(getJobKey(jobId))
                .usingJobData(getJobDataMap())
                .storeDurably(true)
                .build();
    }

    public CronTrigger getCronTrigger(Long jobId, String scheduleCron) {
        return TriggerBuilder.newTrigger()
                .withIdentity(getTriggerKey(jobId))
                .startNow()
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(scheduleCron)
                                .withMisfireHandlingInstructionIgnoreMisfires())
                .build();
    }
}
