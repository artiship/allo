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

package io.github.artiship.allo.database.repository;

import io.github.artiship.allo.database.entity.Task;
import lombok.Data;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** @author jiangpeipei */
@Repository
public interface TaskRepository extends PagingAndSortingRepository<Task, Long> {

    @Query(
            rowMapperClass = RunningJobCountMapper.class,
            value =
                    "select worker,count(*) as cnt from task where worker is not null group by worker")
    List<Map<String, Object>> getRunningJobCount();

    @Query(
            "select * from task where job_id = :jobId and schedule_time = :scheduleTime order by start_time desc")
    List<Task> getInstancesByIdAndTime(Long jobId, String scheduleTime);

    @Query("select * from task where worker_host = :ip and task_state in (:states)")
    List<Task> findAllTaskByWorkerHostAndTaskStates(String ip, List<Integer> states);

    @Query(
            rowMapperClass = AllJobStatusMapper.class,
            value =
                    "select id,job_id,task_state,schedule_time from task where schedule_time is not null and schedule_time and schedule_time > :startTime and schedule_time < :endTime")
    List<Task> getAllJobStatus(String startTime, String endTime);

    @Query("select * from task where task_state in (:states) order by schedule_time asc")
    List<Task> findTasksByTaskStates(List<Integer> states);

    @Query("select * from task where job_id = :jobId and task_state in (:states)")
    List<Task> findTasksByTaskStatesAndJobId(Long jobId, List<Integer> states);

    @Query("select * from task where dag_id = :dagId order by create_time")
    List<Task> findAllByDagId(Long dagId);

    @Modifying
    @Query("update task set task_state = :state where id in (:ids)")
    Integer updateJobStatus(List<Long> ids, Integer state);

    @Modifying
    @Query("delete from task where job_id = :jobId")
    Integer deleteAllByJobId(Long jobId);

    @Modifying
    @Query(
            "update task set task_state = :state where job_id = :id and schedule_time = :scheduleTime")
    Integer updateJobStatusAsFailed(Integer state, Long jobId, String scheduleTime);

    @Query(
            "select * from task where job_id = :jobId and schedule_time < :scheduleTime order by schedule_time, start_time")
    List<Task> getTaskList(Long jobId, String scheduleTime);

    @Query(
            "select b.* from (select max(id) as id from task  where job_id = :id and schedule_time =:time group by schedule_time ,job_id ) a  join  ( select * from task where job_id  = :id and schedule_time = :time ) b on a.id = b.id")
    List<Task> findTaskByChildTime(Long id, String time);

    @Query(
            "select * from task where job_id = :jobId and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :executionDate")
    List<Task> findByExecutionDateByHour(Long jobId, String executionDate);

    @Query(
            "select * from task where job_id = :jobId and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :executionDate")
    List<Task> findByExecutionDateByDay(Long jobId, String executionDate);

    @Query(
            rowMapperClass = CheckReadyMapper.class,
            value =
                    "select id as task_id, task_state, schedule_time, start_time, end_time "
                            + "from task "
                            + "where task_name = :tableName "
                            + "and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :day "
                            + "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByDay(String tableName, String day);

    @Query(
            rowMapperClass = CheckReadyMapper.class,
            value =
                    "select id as task_id, task_state, schedule_time, start_time, end_time "
                            + "from task "
                            + "where task_name = :tableName "
                            + "and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :dayHour "
                            + "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByHour(String tableName, String dayHour);

    @Query(
            rowMapperClass = CheckReadyMapper.class,
            value =
                    "select id as task_id, task_state, schedule_time, start_time, end_time "
                            + "from task "
                            + "where job_id = :jobId "
                            + "and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :day "
                            + "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByJobIdAndDay(Long jobId, String day);

    @Query(
            rowMapperClass = CheckReadyMapper.class,
            value =
                    "select id as task_id, task_state, schedule_time, start_time, end_time "
                            + "from task "
                            + "where job_id = :jobId "
                            + "and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :dayHour "
                            + "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByJobIdAndHour(Long jobId, String dayHour);

    class CheckReadyMapper implements RowMapper<Map<String, Object>> {
        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            HashMap<String, Object> result = new HashMap<>();
            result.put("taskId", rs.getLong("task_id"));
            result.put("taskState", rs.getInt("task_state"));
            result.put("scheduleTime", rs.getTimestamp("schedule_time"));
            result.put("startTime", rs.getTimestamp("start_time"));
            result.put("endTime", rs.getTimestamp("end_time"));
            return result;
        }
    }

    class RunningJobCountMapper implements RowMapper<Map<String, Object>> {
        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            HashMap<String, Object> result = new HashMap<>();
            result.put("worker", rs.getString("worker"));
            result.put("cnt", rs.getLong("cnt"));
            return result;
        }
    }

    class AllJobStatusMapper implements RowMapper<JobTaskState> {
        @Override
        public JobTaskState mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobTaskState jobTaskState = new JobTaskState();
            jobTaskState.setId(rs.getInt("id"));
            jobTaskState.setJobId(rs.getInt("job_id"));
            jobTaskState.setScheduleTime(rs.getDate("schedule_time"));
            jobTaskState.setTaskState(rs.getInt("task_state"));
            return jobTaskState;
        }
    }

    @Data
    class JobTaskState {
        private int id;
        private int jobId;
        private Date scheduleTime;
        private int TaskState;
    }
}
