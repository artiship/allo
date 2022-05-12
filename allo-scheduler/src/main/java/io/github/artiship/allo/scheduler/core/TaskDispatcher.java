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

import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.enums.TaskOperation;
import io.github.artiship.allo.model.enums.TaskState;
import io.github.artiship.allo.model.exception.TaskNotFoundException;
import io.github.artiship.allo.rpc.RpcClient;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.RpcTask;
import io.github.artiship.allo.scheduler.rpc.SchedulerRpcService;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.artiship.allo.model.enums.TaskState.*;
import static io.github.artiship.allo.model.enums.TaskTriggerType.MANUAL_BACK_FILL;
import static io.github.artiship.allo.model.enums.TaskTriggerType.MANUAL_RUN_DOWNSTREAM;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
@Component
public class TaskDispatcher extends AbstractScheduler {
    @Autowired private ResourceManager resourceManager;
    @Autowired private JobStateStore jobStateStore;
    @Autowired private SchedulerDao schedulerDao;
    @Autowired private SchedulerRpcService schedulerRpcService;
    @Autowired private TaskOperationCache taskOperationCache;

    @Value("${services.task-dispatcher.thread.count:1}")
    private int threadCount;

    private ReentrantLock jobConcurrencyLock = new ReentrantLock();

    private ThreadPoolExecutor limitedThreadPool;

    @Override
    public TaskBo submit(TaskBo task) {
        TaskBo taskUpdated =
                this.schedulerDao.saveTask(task.setTaskState(WAITING).setWaitingTime(now()));

        try {
            jobStateStore.removeTaskSuccessRecord(taskUpdated);
        } catch (Exception e) {
            log.info(
                    "Task_{}_{} SUBMIT to task dispatcher remove success records fail,",
                    taskUpdated.getJobId(),
                    taskUpdated.getId(),
                    e);
            return this.schedulerDao.saveTask(task.setTaskState(FAIL).setWaitingTime(now()));
        }

        this.threadPool.execute(new DispatchTaskThread(taskUpdated));

        log.info(
                "Task_{}_{} SUBMIT to task dispatcher: {}, {}, {}",
                taskUpdated.getJobId(),
                taskUpdated.getId(),
                taskUpdated.getTaskTriggerType(),
                taskUpdated.getJobPriority(),
                taskUpdated.getScheduleTime());

        return taskUpdated;
    }

    @Override
    public void start() {
        schedulerRpcService.registerListener(this);
        this.threadPool =
                new ThreadPoolExecutor(
                        threadCount,
                        threadCount,
                        0L,
                        MILLISECONDS,
                        new PriorityBlockingQueue<>(),
                        createThreadFactory("Task-dispatcher-cron"),
                        new DiscardPolicy());

        this.limitedThreadPool =
                new ThreadPoolExecutor(
                        threadCount,
                        threadCount,
                        0L,
                        MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        createThreadFactory("Task-dispatcher-limited"),
                        new DiscardPolicy());

        log.info("Reload waiting tasks to dispatcher - Start...");
        this.schedulerDao
                .getTasksByState(
                        WAITING,
                        WAITING_PARALLELISM_LIMIT,
                        WAITING_TASK_SLOT,
                        WAITING_COMPLEMENT_LIMIT)
                .forEach(
                        t -> {
                            this.threadPool.execute(new DispatchTaskThread(t));
                            log.info(
                                    "Task_{}_{} RELOAD to task dispatcher: {}, {}, {}",
                                    t.getJobId(),
                                    t.getId(),
                                    t.getTaskTriggerType(),
                                    t.getJobPriority(),
                                    t.getScheduleTime());
                        });
        log.info("Reload waiting tasks to dispatcher - Stopped");
    }

    private void updateTaskState(TaskBo task, TaskState state) {
        if (task.getTaskState() != state) {
            schedulerDao.updateTaskState(task.getId(), state);
            task.setTaskState(state);
        }
    }

    private class DispatchTaskThread implements Runnable, Comparable<DispatchTaskThread> {

        private TaskBo task;

        public DispatchTaskThread(TaskBo task) {
            this.task = task;
        }

        @Override
        public void run() {
            log.debug("{} DispatchTaskThread start run", task.traceId());
            if (!tryDispatch()) {
                getExecutor(task.getTaskState()).execute(this);
            }
        }

        private boolean tryDispatch() {
            try {
                if (taskOperationCache.applied(task, TaskOperation.BLOCK)) {
                    schedulerDao.saveTask(task.setTaskState(BLOCKED));
                    return false;
                }

                if (taskOperationCache.applied(task, TaskOperation.UNBLOCK)) {
                    schedulerDao.saveTask(task.setTaskState(WAITING));
                    return false;
                }

                if (taskOperationCache.applied(task, TaskOperation.KILL)) {
                    schedulerDao.saveTask(task.setTaskState(KILLED).setEndTime(now()));
                    return true;
                }

                if (taskOperationCache.applied(task, TaskOperation.MARK_FAIL)) {
                    schedulerDao.saveTask(task.setTaskState(FAIL).setEndTime(now()));
                    return true;
                }

                if (taskOperationCache.applied(task, TaskOperation.MARK_SUCCESS)) {
                    schedulerDao.saveTask(task.setTaskState(SUCCESS).setEndTime(now()));
                    return true;
                }

                if (task.getIsSkipRun()) {
                    schedulerDao.saveTask(
                            task.setTaskState(SUCCESS).setStartTime(now()).setEndTime(now()));
                    jobStateStore.taskSuccess(task);
                    log.info(
                            "Task_{}_{} SKIP RUN, marked as SUCCESS",
                            task.getJobId(),
                            task.getId());
                    return true;
                }

                if (task.getTaskState() == BLOCKED) {
                    return false;
                }

                if (dispatch()) return true;

                task.penalty();

                log.debug(
                        "{} penalty and back to queue: {}, {}, {}",
                        task.traceId(),
                        task.getJobPriority(),
                        task.getPenalty(),
                        task.getScheduleTime());
            } catch (Exception e) {
                log.error("Task_{}_{} dispatch cause exception", task.getJobId(), task.getId(), e);
            }
            return false;
        }

        private ThreadPoolExecutor getExecutor(TaskState taskState) {
            if (asList(WAITING_COMPLEMENT_LIMIT, WAITING_PARALLELISM_LIMIT, BLOCKED)
                    .contains(taskState)) return limitedThreadPool;
            return threadPool;
        }

        private boolean dispatch() {
            if (asList(MANUAL_BACK_FILL, MANUAL_RUN_DOWNSTREAM).contains(task.getTaskTriggerType())
                    && now().getHour() < 10) {
                updateTaskState(task, WAITING_COMPLEMENT_LIMIT);
                return false;
            }

            jobConcurrencyLock.lock();
            try {

                if (jobStateStore.exceedConcurrencyLimit(task.getJobId())) {
                    updateTaskState(task, WAITING_PARALLELISM_LIMIT);
                    return false;
                }

                final Optional<WorkerBo> availableWorker = resourceManager.availableWorker(task);

                if (!availableWorker.isPresent()) {
                    updateTaskState(task, WAITING_TASK_SLOT);
                    return false;
                }

                WorkerBo worker = availableWorker.get();

                try (final RpcClient workerRpcClient =
                        RpcClient.create(worker.getHost(), worker.getPort())) {
                    final RpcTask rpcTask =
                            RpcUtils.toRpcTask(
                                    task.setWorkerHost(worker.getHost())
                                            .setWorkerPort(worker.getPort()));

                    workerRpcClient.submitTask(rpcTask);

                    log.info(
                            "Task_{}_{} DISPATCH to {}: priority={}, penalty={}, schedule_time={}",
                            task.getJobId(),
                            task.getId(),
                            worker.getHost(),
                            task.getJobPriority(),
                            task.getPenalty(),
                            task.getScheduleTime());
                } catch (StatusRuntimeException e) {
                    resourceManager.unHealthyWorker(worker.getHost());
                    log.warn("Worker_{} can't reach, add to un healthy check", worker.getHost(), e);
                    return false;
                }

                try {
                    jobStateStore.addJobRunningTask(task);
                    resourceManager.decrease(worker.getHost());

                    schedulerDao.saveTask(task.setTaskState(DISPATCHED).setDispatchedTime(now()));
                } catch (Exception e) {
                    log.info(
                            "Task_{}_{} update state after dispatched fail",
                            task.getJobId(),
                            task.getId(),
                            e);
                }

                return true;
            } catch (TaskNotFoundException e) {
                log.warn("Task_{}_{} has already deleted.", task.getJobId(), task.getId(), e);
                return true;
            } catch (Exception e) {
                log.error("Task_{}_{} dispatch failed.", task.getJobId(), task.getId(), e);
            } finally {
                jobConcurrencyLock.unlock();
            }

            return false;
        }

        public TaskBo getTask() {
            return task;
        }

        @Override
        public int compareTo(DispatchTaskThread o) {
            return this.task.compareTo(o.getTask());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DispatchTaskThread that = (DispatchTaskThread) o;
            return Objects.equals(task, that.task);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task);
        }
    }

    public long limitedQueuedTaskCount() {
        if (limitedThreadPool == null) return 0;

        return limitedThreadPool.getQueue().size();
    }

    @Override
    public void onFailOver(TaskBo task) {
        TaskBo submit = submit(task.toRenewTask().setRetryTimes(task.getRetryTimes()));

        log.info("{} FAIL_OVER: new task={}.", task.traceId(), submit.traceId());
    }
}
