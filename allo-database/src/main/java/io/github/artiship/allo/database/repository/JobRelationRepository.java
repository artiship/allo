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

import io.github.artiship.allo.database.entity.JobRelation;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRelationRepository extends CrudRepository<JobRelation, Long> {

    @Modifying
    @Query("delete from job_relation where job_id = :id")
    Integer deleteByJobId(Long jobId);

    @Modifying
    @Query("delete from job_relation where job_id = :jobId and parent_job_id = :parentJobId")
    Integer deleteByJobIdAndParentJobId(Long jobId, Long parentJobId);

    @Query("select * from job_relation where job_id = :jobId")
    List<JobRelation> findParentsByJobId(Long jobId);

    @Query("select * from job_relation where job_id = :jobId and parent_job_id != -1")
    List<JobRelation> findParentsByJobIdNoParent(Long jobId);

    @Query("select * from job_relation where parent_job_id= :parentJobId")
    List<JobRelation> findChildrenByParentJobId(Long parentJobId);

    @Query("select count(1) from job_relation where parent_job_id= :parentJobId")
    Integer findChildrenCountByParentJobId(Long parentJobId);

    @Query("select * from job_relation where job_id = :jobId and parent_job_id= :parentJobId")
    List<JobRelation> findIdByJobIdAndParentJobId(Long jobId, Long parentJobId);

    @Query("select * from job_relation")
    List<JobRelation> findAllRelation();

    @Query(
            "select count(1) as num from job_relation where job_id =:jobId and parent_job_id =:parentJobId")
    long findRelationByJobIdAndParentId(Long jobId, Long parentJobId);
}
