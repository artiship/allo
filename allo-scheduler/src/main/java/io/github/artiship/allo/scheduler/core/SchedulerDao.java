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

import com.google.common.collect.ImmutableSet;
import io.github.artiship.allo.database.entity.Job;
import io.github.artiship.allo.database.entity.JobRelation;
import io.github.artiship.allo.database.entity.Task;
import io.github.artiship.allo.database.entity.Worker;
import io.github.artiship.allo.database.repository.JobRelationRepository;
import io.github.artiship.allo.database.repository.JobRepository;
import io.github.artiship.allo.database.repository.WorkerRepository;
import io.github.artiship.allo.database.repository.TaskRepository;
import io.github.artiship.allo.model.bo.JobBo;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.enums.TaskState;
import io.github.artiship.allo.model.exception.JobNotFoundException;
import io.github.artiship.allo.model.exception.TaskNotFoundException;
import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.scheduler.dependency.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Joiner.on;
import static io.github.artiship.allo.model.enums.JobState.OFFLINE;
import static io.github.artiship.allo.model.enums.JobState.ONLINE;
import static io.github.artiship.allo.model.enums.TaskState.*;
import static io.github.artiship.allo.model.enums.WorkerState.DEAD;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Slf4j
@Component
public class SchedulerDao {

    @Autowired private JobRepository jobRepository;
    @Autowired private JobRelationRepository jobRelationRepository;
    @Autowired private TaskRepository taskRepository;
    @Autowired private WorkerRepository workerRepository;
    @Autowired private JdbcTemplate jdbcTemplate;

    private static final String SQL_LATEST_SUCCESS_TASKS =
            "SELECT id, "
                    + " job_id, "
                    + " schedule_cron, "
                    + " schedule_time, "
                    + " (end_time - start_time) AS execution_cost "
                    + "FROM ( "
                    + " SELECT id, "
                    + "  job_id, "
                    + "  schedule_cron, "
                    + "  schedule_time, "
                    + "  task_state, "
                    + "  end_time, "
                    + "  start_time "
                    + " FROM ( "
                    + "  SELECT id, "
                    + "   job_id, "
                    + "   schedule_cron, "
                    + "   schedule_time, "
                    + "   task_state, "
                    + "   end_time, "
                    + "   start_time "
                    + "  FROM task "
                    + "  WHERE job_id = ? "
                    + "  ORDER BY update_time DESC limit 100000 "
                    + "  ) AS t "
                    + " GROUP BY t.schedule_time "
                    + " ) x "
                    + "WHERE x.task_state = "
                    + SUCCESS.getCode()
                    + " "
                    + "ORDER BY schedule_time DESC limit ?";

    public JobBo getJobAndDependencies(Long jobId) {
        JobBo JobBo = this.getJob(jobId);
        List<JobBo> dependencies = this.getJobDependencies(JobBo.getId());
        JobBo.addDependencies(dependencies);

        return JobBo;
    }

    public List<JobBo> getJobDependencies(Long jobId) {
        List<JobRelation> parents = jobRelationRepository.findParentsByJobId(jobId);

        if (parents != null && !parents.isEmpty()) {
            return this.getJobs(parents.stream().map(i -> i.getParentJobId()).collect(toList()));
        }

        return null;
    }

    public List<JobBo> getJobs(List<Long> jobIds) {
        List<Job> parentJobs =
                jobRepository.findAllByIdsAndJobReleaseState(jobIds, ONLINE.getCode());

        if (parentJobs != null && !parentJobs.isEmpty()) {
            return parentJobs.stream().map(i -> i.toJobBo()).collect(toList());
        }

        return null;
    }

    public JobBo getJob(Long jobId) {
        return jobRepository
                .findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId))
                .toJobBo();
    }

    public TaskBo saveTask(TaskBo taskBo) {
        Task schedulerTask = Task.from(taskBo);
        schedulerTask.setUpdateTime(now());
        final Long taskId = schedulerTask.getId();
        if (taskId == null) {
            schedulerTask.setCreateTime(schedulerTask.getUpdateTime());
            return taskRepository.save(schedulerTask).toTaskBo();
        }

        Task schedulerTaskFromDb =
                taskRepository
                        .findById(taskId)
                        .orElseThrow(() -> new TaskNotFoundException(taskId));

        return taskRepository
                .save(schedulerTaskFromDb.updateIgnoreNull(schedulerTask))
                .toTaskBo();
    }

    public List<TaskBo> getRunningTasksByWorker(String ip) {
        List<TaskBo> tasks = emptyList();
        List<Task> runningTasks =
                taskRepository.findAllTaskByWorkerHostAndTaskStates(
                        ip, asList(DISPATCHED.getCode(), RUNNING.getCode()));
        if (runningTasks != null) {
            tasks = runningTasks.stream().map(Task::toTaskBo).collect(toList());
        }
        return tasks;
    }

    public List<TaskBo> getTasksByState(TaskState... states) {
        List<TaskBo> tasks = emptyList();
        List<Task> runningTasks =
                taskRepository.findTasksByTaskStates(
                        asList(states).stream().map(TaskState::getCode).collect(toList()));
        if (runningTasks != null) {
            tasks = runningTasks.stream().map(Task::toTaskBo).collect(toList());
        }
        return tasks;
    }

    public WorkerBo saveNode(WorkerBo worker) {
        requireNonNull(worker, "Worker is null");
        requireNonNull(worker.getHost(), "Worker host is null");
        requireNonNull(worker.getPort(), "Worker port is null");

        Worker workerFromDb =
                workerRepository.findWorkerByHostAndPort(worker.getHost(), worker.getPort());
        Worker schedulerNode = Worker.from(worker);
        if (workerFromDb == null) {
            schedulerNode.setCreateTime(now()).setUpdateTime(now());
            return workerRepository.save(schedulerNode).toWorkerBo();
        }

        return workerRepository.save(workerFromDb.updateNotNull(schedulerNode)).toWorkerBo();
    }

    public void updateWorkerDead(String ip, Integer port) {
        Worker workerFromDb = workerRepository.findWorkerByHostAndPort(ip, port);
        if (workerFromDb != null) {
            workerFromDb.setWorkerState(DEAD.getCode());
            workerRepository.save(workerFromDb);
        }
    }

    public List<JobRelation> getJobRelations() {
        List<JobRelation> relations = emptyList();
        try {
            String sql =
                    "select job_id, parent_job_id "
                            + "from job_relation a "
                            + "join job b on a.job_id = b.id "
                            + "where b.job_release_state in ("
                            + OFFLINE.getCode()
                            + ", "
                            + ONLINE.getCode()
                            + ")";

            relations = jdbcTemplate.query(sql, new BeanPropertyRowMapper(JobRelation.class));
        } catch (Exception e) {
            log.error("Get job relations fail", e);
        }
        return relations;
    }

    public List<TaskSuccessRecord> getLatestSuccessTasks(Long jobId, Integer size) {
        List<TaskSuccessRecord> history = emptyList();
        try {
            history =
                    jdbcTemplate.query(
                            SQL_LATEST_SUCCESS_TASKS,
                            new Object[] {jobId, size},
                            (rs, i) ->
                                    TaskSuccessRecord.of(
                                            rs.getLong("id"),
                                            rs.getString("schedule_cron"),
                                            rs.getTimestamp("schedule_time").toLocalDateTime(),
                                            rs.getLong("execution_cost")));
        } catch (Exception e) {
            log.error("Get latest success tasks of job {} limit {} fail", jobId, size, e);
        }
        return history;
    }

    public TaskBo getTask(Long taskId) {
        Task schedulerTask =
                taskRepository
                        .findById(taskId)
                        .orElseThrow(() -> new TaskNotFoundException(taskId));
        return schedulerTask.toTaskBo();
    }

    public List<String> getLatestFailedHosts(Long jobId, Integer size) {
        List<String> hosts = emptyList();
        try {
            String sql =
                    "select worker_host "
                            + "from task "
                            + "where job_id = "
                            + jobId
                            + " "
                            + "and worker_host is not null "
                            + "and task_state = "
                            + FAIL.getCode()
                            + " "
                            + "order by update_time desc "
                            + "limit "
                            + size;
            hosts = jdbcTemplate.query(sql, (rs, i) -> rs.getString("worker_host"));
        } catch (Exception e) {
            log.error("Get latest failed hosts of job {} limit {} fail", jobId, size, e);
        }
        return hosts;
    }

    public boolean isTaskTheFirstInstanceOfJob(Long jobId, LocalDateTime scheduleTime) {
        boolean isFirst = false;
        try {
            String sql =
                    "select count(1) counts "
                            + "from task "
                            + "where job_id = "
                            + jobId
                            + " "
                            + "and schedule_time < '"
                            + TimeUtils.toStr(scheduleTime)
                            + "' ";
            isFirst = jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("counts")) == 0;
        } catch (Exception e) {
            log.error("Check schedule time {} if the first of job {}", scheduleTime, jobId, e);
        }

        return isFirst;
    }

    public boolean isJobSelfDepend(Long jobId) {
        boolean is_self_dependent = false;
        try {
            String sql =
                    "select is_self_dependent "
                            + "from job "
                            + "where id = "
                            + jobId;
            is_self_dependent =
                    jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("is_self_dependent"))
                            == 1;
        } catch (Exception e) {
            log.error("Check job {} if the self depend", jobId, e);
        }

        return is_self_dependent;
    }

    public List<Long> getJobConcurrentTasks(Long jobId) {
        List<Long> tasks = emptyList();
        try {
            String sql =
                    "select id "
                            + "from task "
                            + "where job_id = "
                            + jobId
                            + " "
                            + "and task_state in ("
                            + DISPATCHED.getCode()
                            + ","
                            + RUNNING.getCode()
                            + ")";

            tasks = jdbcTemplate.query(sql, (rs, i) -> rs.getLong("id"));
        } catch (Exception e) {
            log.error("Get job {} running tasks fail", jobId, e);
        }
        return tasks;
    }

    public List<Long> getSourceHostConcurrentTasks(String sourceHost) {
        List<Long> tasks = emptyList();
        try {
            String sql =
                    "select id "
                            + "from task "
                            + "where source_host = '"
                            + sourceHost
                            + "' "
                            + "and task_state in ("
                            + DISPATCHED.getCode()
                            + ","
                            + RUNNING.getCode()
                            + ")";

            tasks = jdbcTemplate.query(sql, (rs, i) -> rs.getLong("id"));
        } catch (Exception e) {
            log.warn("Get source host {} running tasks fail", sourceHost, e);
        }
        return tasks;
    }

    public Optional<Long> getJobIdByTaskId(Long taskId) {
        Long jobId = null;
        try {
            String sql =
                    "select job_id "
                            + "from task "
                            + "where id = "
                            + taskId
                            + " "
                            + "limit 1";

            jobId = jdbcTemplate.queryForObject(sql, Long.class);
        } catch (Exception e) {
            log.error("Get job id by task id {} tasks fail", taskId, e);
        }
        return Optional.ofNullable(jobId);
    }

    public boolean dagHasJobSuccessOrUnfinished(
            Long dagId, Long jobId, LocalDateTime scheduleTime) {
        boolean has = false;
        List<Integer> filterStates = unFinishedStateCodes();
        filterStates.add(SUCCESS.getCode());
        try {
            String sql =
                    "select count(1) counts "
                            + "from task "
                            + "where dag_id = "
                            + dagId
                            + " "
                            + "and job_id = "
                            + jobId
                            + " "
                            + "and task_state in ("
                            + on(",").join(filterStates)
                            + ") "
                            + "and schedule_time = str_to_date('"
                            + TimeUtils.toStr(scheduleTime)
                            + "', '%Y-%m-%d %H:%i:%s')";
            has = jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("counts")) > 0;
        } catch (Exception e) {
            log.error(
                    "Check if dag {} job {} at schedule time {} have success record fail",
                    dagId,
                    jobId,
                    scheduleTime,
                    e);
        }

        return has;
    }

    public Optional<Long> getDuplicateTask(Long jobId, LocalDateTime scheduleTime) {
        try {
            String sql =
                    "select id "
                            + "from task "
                            + "where job_id = "
                            + jobId
                            + " "
                            + "and task_state not in ("
                            + finishTaskState()
                            + ") "
                            + "and schedule_time = str_to_date('"
                            + TimeUtils.toStr(scheduleTime)
                            + "', '%Y-%m-%d %H:%i:%s') "
                            + "order by id desc limit 1";
            return Optional.ofNullable(
                    jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getLong("id")));
        } catch (EmptyResultDataAccessException e) {
            log.debug("Job has no duplicate task");
        } catch (Exception e) {
            log.error("Check job {} at schedule time {} duplication fail", jobId, scheduleTime, e);
        }
        return Optional.empty();
    }

    public Set<LocalDateTime> getDuplicateScheduleTimes(Long jobId) {
        try {
            String sql =
                    "select schedule_time "
                            + "from task "
                            + "where job_id = "
                            + jobId
                            + " "
                            + "and task_state not in ("
                            + finishTaskState()
                            + ") ";

            return jdbcTemplate.queryForList(sql).stream()
                    .map(i -> ((Timestamp) i.get("schedule_time")).toLocalDateTime())
                    .collect(Collectors.toSet());

        } catch (EmptyResultDataAccessException e) {
            log.debug("Job has no duplicate task");
        } catch (Exception e) {
            log.error("Get job {} duplication fail", jobId, e);
        }
        return ImmutableSet.of();
    }

    private String finishTaskState() {
        return on(",").join(TaskState.finishStateCodes());
    }

    public String getJobCron(Long jobId) {
        String cron = null;
        try {
            String sql =
                    "select schedule_cron "
                            + "from job "
                            + "where id = "
                            + jobId;

            cron = jdbcTemplate.queryForObject(sql, String.class);
        } catch (Exception e) {
            log.error("Get job {} cron expression fail", cron, e);
        }

        return cron;
    }

    public Set<Long> getJobChildren(Long jobId) {
        List<JobRelation> children =
                this.jobRelationRepository.findChildrenByParentJobId(jobId);
        if (children == null) return emptySet();
        return children.stream().map(JobRelation::getJobId).collect(toSet());
    }

    public void updateTaskDependencies(Long taskId, String taskDependencies) {
        try {
            String sql =
                    "update task set dependencies_json = '"
                            + taskDependencies
                            + "' "
                            + "where id = "
                            + taskId;

            jdbcTemplate.update(sql);
        } catch (Exception e) {
            log.error("Update task {} dependencies json {} fail", taskId, taskDependencies, e);
        }
    }

    public void updateTaskState(Long taskId, TaskState state) {
        requireNonNull(state, "State should not be null.");

        try {
            String sql =
                    "update task set task_state = "
                            + state.getCode()
                            + " "
                            + "where id = "
                            + taskId;

            jdbcTemplate.update(sql);
        } catch (Exception e) {
            log.error("Update task {} state {} fail", taskId, state, e);
        }
    }

    public TaskState getTaskStateById(Long taskId) {
        try {
            String sql = "select task_state from task where id = " + taskId;
            Integer taskStateCode = jdbcTemplate.queryForObject(sql, Integer.class);
            return TaskState.of(taskStateCode);
        } catch (Exception e) {
            log.error("Get task_{} state fail", taskId, e);
        }
        return UNKNOWN;
    }
}
