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

import io.github.artiship.allo.database.entity.Job;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends CrudRepository<Job, Long> {
    @Query(
            "select * "
                    + "from job a "
                    + "left join job_relation b on a.id = b.parent_job_id "
                    + "where a.job_release_state in (0,1) "
                    + "and b.id in (:ids)")
    List<Job> getJobParentsByIds(List<Long> ids);

    @Query(
            "select * "
                    + "from job a "
                    + "left join job_relation b on a.id = b.job_id "
                    + "where a.job_release_state in (0,1) "
                    + "and b.id in (:ids)")
    List<Job> getJobChildsByIds(List<Long> ids);

    @Query("select * from job where id = :id and job_release_state = :jobReleaseState")
    Job findByIdAndJobReleaseState(Long id, Integer jobReleaseState);

    @Modifying
    @Query("update job set job_release_state = :state where id in (:ids)")
    Integer updateJobReleaseStatusByIds(List<Long> ids, Integer state);

    @Modifying
    @Query("update job set job_release_state = :state where id =:id")
    Integer updateJobReleaseStatusById(Long id, Integer state);

    @Modifying
    @Query(
            "update job set version = :version, oss_path = :ossPath, job_configuration = :versionContent where id =:id")
    Integer updateJobVersionById(Long id, String version, String ossPath, String versionContent);

    @Query("select * from job where id in (:ids) and job_release_state = :jobReleaseState")
    List<Job> findAllByIdsAndJobReleaseState(List<Long> ids, Integer jobReleaseState);

    @Modifying
    @Query("delete from job_relation where job_id = :id and parent_job_id = :parent_job_id")
    Integer deleteByJobId(Long jobId, Long parent_job_id);

    @Query("select * from job where job_name = :jobName and job_release_state != -2 ")
    List<Job> findJobsByName(String jobName);

    @Query(
            "select * from job where job_name = :jobName and job_release_state != -2 and id != :jobId")
    List<Job> findJobsByNameAndId(Long jobId, String jobName);

    @Query("select * from job where job_release_state != -2 and job_type in (:integers)")
    List<Job> findAllByTag(List<Integer> integers);

    @Query(
            "select * from job where job_type = 9 and job_release_state != -2 and json_extract(job_configuration,'$.reportId') = :tableauId")
    List<Job> queryTableauJobByTableauId(String tableauId);

    @Query("select * from job where job_release_state != -2")
    List<Job> queryJobs();

    @Query("select count(1) from job where job_name = :tableName and job_release_state != -2 ")
    Integer queryCountByTableName(String tableName);
}
