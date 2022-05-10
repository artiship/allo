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

import io.github.artiship.allo.database.entity.Worker;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkerRepository extends CrudRepository<Worker, Long> {

    @Modifying
    @Query(
            "update worker set worker_status = :status where worker_group= :nodeGroup and worker_host in (:worker)")
    Integer updateWorkersStatusByGroup(Integer status, String nodeGroup, List<String> worker);

    @Query("select * from worker where worker_group= :nodeGroup and worker_host in (:worker)")
    List<Worker> getWorkersInfoByGroup(String nodeGroup, List<String> worker);

    @Query("select * from worker where worker_host = :host and worker_rpc_port = :rpcPort")
    Worker findWorkerByHostAndPort(String host, Integer rpcPort);
}
