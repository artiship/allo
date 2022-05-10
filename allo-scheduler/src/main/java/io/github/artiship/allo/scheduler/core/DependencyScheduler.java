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

package io.github.artiship.allo.scheduler.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.enums.TaskOperation;
import io.github.artiship.allo.model.exception.JobNotFoundException;
import io.github.artiship.allo.model.exception.TaskNotFoundException;
import io.github.artiship.allo.quartz.QuartzJob;
import io.github.artiship.allo.quartz.QuartzListener;
import io.github.artiship.allo.tra.exception.CronNotSatisfiedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.artiship.allo.model.enums.TaskState.*;
import static io.github.artiship.allo.model.enums.TaskTriggerType.MANUAL_FREE;
import static java.time.LocalDateTime.now;

@Slf4j
@Component
public class DependencyScheduler extends AbstractScheduler implements QuartzListener {
    @Value("${services.task-scheduler.thread.count:2}")
    private int threadCount;

    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private TaskOperationCache taskOperationCache;

    @Override
    public TaskBo submit(TaskBo task) {
        TaskBo taskUpdated = schedulerDao.saveTask(task.setTaskState(PENDING)
                                                                .setPendingTime(now()));
        taskUpdated.setSkipDependencies(task.getSkipDependencies());

        try {
            jobStateStore.removeTaskSuccessRecord(taskUpdated);
        } catch (Exception e) {
            log.info("Task_{}_{} SUBMIT to task scheduler remove success records fail,",
                    taskUpdated.getJobId(), taskUpdated.getId(), e);
            return this.schedulerDao.saveTask(task.setTaskState(FAIL)
                                                  .setWaitingTime(now()));
        }

        this.threadPool.execute(new ScheduleTaskThread(taskUpdated));

        log.info("Task_{}_{} SUBMIT to task scheduler: {}, {}, {}",
                taskUpdated.getJobId(), taskUpdated.getId(),
                taskUpdated.getTaskTriggerType(), taskUpdated.getJobPriority(), taskUpdated.getScheduleTime());

        return taskUpdated;
    }

    @Override
    public void onTrigger(TaskBo task) {
        submit(task);
    }

    protected boolean shouldFree(final TaskBo task) {
        if (taskOperationCache.applied(task, TaskOperation.FREE)) {
            schedulerDao.saveTask(task.setTaskTriggerType(MANUAL_FREE));
            return true;
        }
        return false;
    }

    @Override
    public void start() {
        QuartzJob.register(this);

        threadPool = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("Task-scheduler-%d")
                                                                       .build(),
                new ThreadPoolExecutor.DiscardPolicy());


        log.info("DependencyScheduler restore start...");
        schedulerDao.getTasksByState(PENDING, BLOCKED)
                    .forEach(t -> {
                        threadPool.submit(new ScheduleTaskThread(t));
                        log.info("{} {} RELOAD.", t.traceId(), t.getTaskState());
                    });
        log.info("DependencyScheduler restore end.");
    }

    private class ScheduleTaskThread implements Runnable {
        private TaskBo task;

        public ScheduleTaskThread(TaskBo task) {
            this.task = task;
        }

        @Override
        public void run() {
            if (shouldComplete()) return;
            if (!dispatch()) {
                threadPool.execute(this);
            }
        }

        private boolean shouldComplete() {
            if (taskOperationCache.applied(task, TaskOperation.MARK_FAIL)) {
                schedulerDao.saveTask(task.setTaskState(FAIL).setEndTime(now()));
                return true;
            }

            if (taskOperationCache.applied(task, TaskOperation.MARK_SUCCESS)) {
                schedulerDao.saveTask(task.setTaskState(SUCCESS).setEndTime(now()));
                return true;
            }

            if (taskOperationCache.applied(task, TaskOperation.KILL)) {
                schedulerDao.saveTask(task.setTaskState(KILLED).setEndTime(now()));
                return true;
            }

            if (task.isGarbage()) {
                schedulerDao.saveTask(task.setTaskState(KILLED).setEndTime(now()));
                log.warn("Task_{}_{} GARBAGE and KILLED.", task.getJobId(), task.getId());
                return true;
            }
            return false;
        }

        private boolean dispatch() {
            if (taskOperationCache.applied(task, TaskOperation.BLOCK)) {
                schedulerDao.saveTask(task.setTaskState(BLOCKED));
                return false;
            }

            if (taskOperationCache.applied(task, TaskOperation.UNBLOCK)) {
                schedulerDao.saveTask(task.setTaskState(PENDING));
            }

            if (task.getTaskState() == BLOCKED) {
                return false;
            }

            try {
                if (shouldFree(task) || jobStateStore.isDependencyReady(task)) {
                    taskDispatcher.submit(task);
                    return true;
                }
            } catch (TaskNotFoundException e) {
                log.warn("Task_{}_{} has already deleted.", task.getJobId(), task.getId(), e);
                return true;
            } catch (JobNotFoundException | CronNotSatisfiedException e) {
                log.error("Task_{}_{} check dependencies fail: schedule time {}",
                        task.getJobId(), task.getId(), task.getScheduleTime(), e);

                schedulerDao.saveTask(task.setEndTime(now())
                                          .setTaskState(FAIL));
                return true;
            } catch (Exception e) {
                log.error("Task_{}_{} check dependencies fail: schedule time {}",
                        task.getJobId(), task.getId(), task.getScheduleTime(), e);
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            return this.task.equals(o);
        }

        @Override
        public int hashCode() {
            return task.hashCode();
        }
    }
}
