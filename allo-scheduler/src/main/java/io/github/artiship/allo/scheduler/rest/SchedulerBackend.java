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

package io.github.artiship.allo.scheduler.rest;

import io.github.artiship.allo.database.entity.Job;
import io.github.artiship.allo.database.entity.JobRelation;
import io.github.artiship.allo.database.entity.Task;
import io.github.artiship.allo.database.repository.JobRelationRepository;
import io.github.artiship.allo.database.repository.JobRepository;
import io.github.artiship.allo.database.repository.TaskRepository;
import io.github.artiship.allo.model.bo.JobBo;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.enums.*;
import io.github.artiship.allo.model.exception.JobNotFoundException;
import io.github.artiship.allo.model.ha.ZkLostTask;
import io.github.artiship.allo.quartz.QuartzSchedulerService;
import io.github.artiship.allo.quartz.utils.QuartzUtils;
import io.github.artiship.allo.rpc.RpcClient;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.RpcResponse;
import io.github.artiship.allo.rpc.api.RpcTask;
import io.github.artiship.allo.scheduler.core.*;
import io.github.artiship.allo.scheduler.dependency.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

import static io.github.artiship.allo.model.enums.JobPriority.HIGH;
import static io.github.artiship.allo.model.enums.TaskState.SUCCESS;
import static java.time.LocalDateTime.now;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Slf4j
@Service
public class SchedulerBackend {
    @Autowired private QuartzSchedulerService quartzScheduler;
    @Autowired private JobStateStore jobStateStore;
    @Autowired private SchedulerDao schedulerDao;
    @Autowired private TaskDispatcher taskDispatcher;
    @Autowired private DependencyScheduler dependencyScheduler;
    @Autowired private ResourceManager resourceManager;
    @Autowired private TaskRepository taskRepository;
    @Autowired private JobRepository jobRepository;
    @Autowired private JobRelationRepository jobRelationRepository;
    @Autowired private RetryScheduler retryScheduler;
    @Autowired private TaskOperationCache taskOperationCache;

    private static final Long NO_TASK_ID = -11L;

    public void scheduleJob(Long jobId) {
        JobBo job = schedulerDao.getJobAndDependencies(jobId);
        quartzScheduler.scheduleJob(job);
    }

    public Job scheduleJob(Job schedulerJob) {
        if (schedulerJob.getJobState() == null) {
            schedulerJob.setJobState(JobState.ONLINE.getCode());
        }

        Job job = saveJob(schedulerJob);

        if (schedulerJob.getJobState() == JobState.ONLINE.getCode()) {
            quartzScheduler.scheduleJob(job.toJobBo());
        } else if (schedulerJob.getJobState() == JobState.OFFLINE.getCode()) {
            quartzScheduler.pauseJob(job.toJobBo());
        }
        return job;
    }

    public void pauseJob(Long jobId) {
        Job job = loadJob(jobId);
        quartzScheduler.pauseJob(job.toJobBo());
        job.setJobState(JobState.OFFLINE.getCode());
        saveJob(job);
    }

    public void resumeJob(Long jobId) {
        Job job = loadJob(jobId);
        quartzScheduler.resumeJob(job.toJobBo());

        job.setJobState(JobState.ONLINE.getCode());
        saveJob(job);
    }

    public void deleteJob(Long jobId) {
        Job job = loadJob(jobId);
        quartzScheduler.removeJob(job.toJobBo());
        jobStateStore.removeJob(jobId);

        job.setJobState(JobState.DELETED.getCode());
        saveJob(job);
    }

    // run now
    public Long runJob(Long jobId) {
        TaskBo task = TaskBo.from(schedulerDao.getJob(jobId)).setTaskTriggerType(TaskTriggerType.MANUAL_RUN);

        return schedulerDao
                .getDuplicateTask(task.getJobId(), task.getScheduleTime())
                .orElseGet(
                        () -> {
                            TaskBo submit = taskDispatcher.submit(task);
                            return submit.getId();
                        });
    }

    // run at a point in time
    public Long runJob(Long jobId, LocalDateTime pointInTime) {
        if (pointInTime.isAfter(now())) return NO_TASK_ID;

        TaskBo task = TaskBo.from(schedulerDao.getJob(jobId));
        LocalDateTime preScheduleTime =
                QuartzUtils.preScheduleTimeOfSomeTime(task.getScheduleCron(), pointInTime);

        if (preScheduleTime.isAfter(now())) return NO_TASK_ID;

        task.setTaskTriggerType(TaskTriggerType.MANUAL_RUN).setScheduleTime(preScheduleTime);

        return schedulerDao
                .getDuplicateTask(task.getJobId(), task.getScheduleTime())
                .orElseGet(
                        () -> {
                            TaskBo submit = taskDispatcher.submit(task);
                            return submit.getId();
                        });
    }

    public Long rerunTask(Long taskId) {
        TaskBo task = schedulerDao.getTask(taskId);

        return schedulerDao
                .getDuplicateTask(task.getJobId(), task.getScheduleTime())
                .orElseGet(
                        () -> {
                            JobBo job = schedulerDao.getJob(task.getJobId());

                            TaskBo submit =
                                    dependencyScheduler.submit(
                                            task.toRenewTask()
                                                    .setOssPath(job.getOssPath())
                                                    .setWorkerGroups(job.getWorkerGroups())
                                                    .setRetryTimes(0)
                                                    .setTaskTriggerType(TaskTriggerType.MANUAL_RERUN)
                                                    .setJobPriority(
                                                            task.getTaskTriggerType()
                                                                            == TaskTriggerType.MANUAL_BACK_FILL
                                                                    ? JobPriority.LOW
                                                                    : HIGH));

                            log.info(
                                    "Task_{}_{} CREATED by {}: priority={}, schedule_time={}.",
                                    submit.getJobId(),
                                    submit.getId(),
                                    submit.getTaskTriggerType(),
                                    submit.getJobPriority(),
                                    submit.getScheduleTime());

                            return submit.getId();
                        });
    }

    public boolean killTask(Long taskId) {
        TaskBo task = schedulerDao.getTask(taskId);

        if (killTask(taskId, TaskOperation.KILL)) {
            this.schedulerDao.updateTaskState(task.getId(), TaskState.KILLED);
            jobStateStore.taskKilled(task);
            return true;
        }
        return true;
    }

    public boolean killTask(Long taskId, TaskOperation operation) {
        return killTask(schedulerDao.getTask(taskId), operation);
    }

    public boolean killTask(TaskBo task, TaskOperation operation) {
        if (task.isTerminated()) {
            return true;
        }

        this.taskOperationCache.async(task, operation);

        if (task.getTaskState() == TaskState.RUNNING) {
            RpcTask rpcTask = RpcUtils.toRpcTask(task);
            try (RpcClient rpcClient = RpcClient.create(task.getWorkerHost(), task.getWorkerPort())) {
                log.info("{} SEND to {} to kill", task.traceId(), task.getWorkerHost());
                RpcResponse response = rpcClient.killTask(rpcTask);

                if (response.getCode() != 200) {
                    log.info("{} SEND to {} kill fail", task.traceId(), task.getWorkerHost());
                    return false;
                }

                log.info("{} SEND to {} kill success", task.traceId(), task.getWorkerHost());
                return true;
            } catch (Exception e) {
                log.info("{} SEND to {} kill fail", task.traceId(), task.getWorkerHost(), e);
            }
        }

        if (task.getTaskState() == TaskState.RETRYING) {
            retryScheduler.kill(task);
            log.info("{} SEND to RetryScheduler {} success", task.traceId(), operation);
            return true;
        }

        return false;
    }

    public boolean markTaskSuccess(Long taskId) {
        TaskBo task = schedulerDao.getTask(taskId);
        killTask(taskId, TaskOperation.MARK_SUCCESS);

        schedulerDao.updateTaskState(task.getId(), SUCCESS);
        jobStateStore.taskSuccess(task);

        log.info("Task_{}_{} MARKED as success", task.getJobId(), task.getId());
        return true;
    }

    public boolean markTaskFail(Long taskId) {
        TaskBo task = schedulerDao.getTask(taskId);
        killTask(taskId, TaskOperation.MARK_FAIL);

        schedulerDao.updateTaskState(task.getId(), TaskState.FAIL);
        jobStateStore.taskFailed(task);

        log.info("Task_{}_{} MARKED as fail", task.getJobId(), task.getId());
        return true;
    }

    public void shutdownWorker(String workerHost) {
        try {
            resourceManager.shutdownWorker(workerHost);
        } catch (Exception e) {
            log.error("Worker_{} shutdown fail", workerHost, e);
        }
    }

    public void resumeWorker(String workerHost) {
        try {
            resourceManager.resumeWorker(workerHost);
        } catch (Exception e) {
            log.error("Worker_{} resume fail", workerHost, e);
        }
    }

    public void updateLostTask(ZkLostTask zkLostTask) {
        log.info("Lost task {} found from zk", zkLostTask);
        if (zkLostTask.getState() == SUCCESS) {
            TaskBo task = schedulerDao.saveTask(TaskBo.from(zkLostTask));
            try {
                jobStateStore.taskSuccess(task);
            } catch (Exception e) {
                log.warn(
                        "Add success record to job state store fail: taskId={}, id={}",
                        e,
                        task.getId(),
                        task.getJobId());
            }
        } else if (zkLostTask.getState() == TaskState.FAIL) {
            TaskBo task = schedulerDao.getTask(zkLostTask.getId());
            jobStateStore.addFailedHost(task.getJobId(), task.getWorkerHost());
        }
    }

    public void failoverTasks(String workerHost) {
        schedulerDao
                .getRunningTasksByWorker(workerHost)
                .forEach(
                        task -> {
                            jobStateStore.taskFailed(task);
                            schedulerDao.updateTaskState(task.getId(), TaskState.FAIL);
                            log.info("Task_{}_{} fail over", task.getJobId(), task.getId());
                            taskDispatcher.submit(task.toRenewTask().setTaskTriggerType(TaskTriggerType.FAIL_OVER));
                        });
    }

    public void addJobDependencies(Long jobId, List<JobRelation> relations) {
        requireNonNull(jobId, "Job id is null");

        if (relations == null) return;

        for (JobRelation relation : relations) {
            this.jobStateStore.addJobDependency(relation);
            this.jobRelationRepository.deleteByJobIdAndParentJobId(
                    relation.getJobId(), relation.getParentJobId());
            this.jobRelationRepository.save(
                    JobRelation.of(relation.getJobId(), relation.getParentJobId()));
        }
    }

    public void removeJobDependencies(Long jobId, List<JobRelation> relations) {
        requireNonNull(jobId, "Job id is null");

        if (relations == null) return;

        for (JobRelation relation : relations) {
            this.jobStateStore.removeJobDependency(relation);
            this.jobRelationRepository.deleteByJobIdAndParentJobId(
                    relation.getJobId(), relation.getParentJobId());
        }
    }

    public void removeJobDependency(Long jobId, Long parentJobId) {
        this.jobStateStore.removeJobDependency(jobId, parentJobId);
        this.jobRelationRepository.deleteByJobIdAndParentJobId(jobId, parentJobId);
    }

    public void freeTask(Long taskId) {
        taskOperationCache.async(taskId, TaskOperation.FREE);
    }

    public Set<TaskSuccessRecord> getJobSuccesses(Long jobId) {
        return this.jobStateStore.getJobSuccessHistory(jobId);
    }

    public List<WorkerBo> getWorkers() {
        return this.resourceManager.getWorkers();
    }

    public Set<Long> getRunningTasks(Long jobId) {
        return this.jobStateStore.getJobRunningTasks(jobId);
    }

    public Job getJob(Long jobId) {
        return jobRepository.findById(jobId).get();
    }

    public List<Job> getJobs(List<Long> jobIds) {
        List<Job> result = new ArrayList<>();
        Iterable<Job> iterable = jobRepository.findAllById(jobIds);
        if (iterable != null) {
            iterable.forEach(result::add);
        }
        return result;
    }

    public List<Job> getParentJobs(Long jobId) {
        List<JobRelation> parentJobs =
                this.jobRelationRepository.findParentsByJobId(jobId);
        if (parentJobs == null || parentJobs.isEmpty()) {
            return Collections.emptyList();
        }

        return jobRepository.getJobParentsByIds(
                parentJobs.stream().map(i -> i.getId()).collect(toList()));
    }

    public List<Job> getChildJobs(Long jobId) {
        List<JobRelation> parentJobs =
                this.jobRelationRepository.findChildrenByParentJobId(jobId);
        if (parentJobs == null || parentJobs.isEmpty()) {
            return Collections.emptyList();
        }

        return jobRepository.getJobChildsByIds(
                parentJobs.stream().map(i -> i.getId()).collect(toList()));
    }

    public Task getTask(Long taskId) {
        return this.taskRepository.findById(taskId).get();
    }

    public Set<TaskDependency> getTaskDependencies(Long taskId) {
        return jobStateStore.acquireTaskDependencies(schedulerDao.getTask(taskId));
    }

    public Job loadJob(Long jobId) {
        return jobRepository
                .findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    public Job saveJob(Job schedulerJob) {
        schedulerJob.setUpdateTime(now());
        if (schedulerJob.getId() == null) {
            schedulerJob.setCreateTime(schedulerJob.getCreateTime());
            schedulerJob.setNew(true);
            return jobRepository.save(schedulerJob);
        } else {
            Optional<Job> optional = jobRepository.findById(schedulerJob.getId());
            if (!optional.isPresent()) {
                schedulerJob.setCreateTime(schedulerJob.getCreateTime());
                schedulerJob.setNew(true);
                return jobRepository.save(schedulerJob);
            } else {
                return jobRepository.save(optional.get().updateIgnoreNull(schedulerJob));
            }
        }
    }

    public void blockTask(Long taskId) {
        taskOperationCache.async(taskId, TaskOperation.BLOCK);
    }

    public void unblockTask(Long taskId) {
        taskOperationCache.async(taskId, TaskOperation.UNBLOCK);
    }
}
