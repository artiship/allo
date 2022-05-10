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

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.github.artiship.allo.model.ha.ZkWorker;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.bo.WorkerBo;
import io.github.artiship.allo.model.enums.WorkerState;
import io.github.artiship.allo.rpc.RpcClient;
import io.github.artiship.allo.rpc.api.HealthCheckRequest;
import io.github.artiship.allo.rpc.api.HealthCheckResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.rholder.retry.StopStrategies.stopAfterDelay;
import static com.github.rholder.retry.WaitStrategies.incrementingWait;
import static io.github.artiship.allo.model.enums.WorkerState.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
public class ResourceManager {
    private final Map<String, WorkerBo> workers = new ConcurrentHashMap<>();

    @Autowired private JobStateStore jobStateStore;
    @Autowired private SchedulerDao schedulerDao;

    private Retryer<Boolean> healthCheckRetryer =
            RetryerBuilder.<Boolean>newBuilder()
                    .retryIfResult(Predicates.isNull())
                    .retryIfExceptionOfType(Exception.class)
                    .retryIfRuntimeException()
                    .withWaitStrategy(incrementingWait(100, MILLISECONDS, 10, SECONDS))
                    .withStopStrategy(stopAfterDelay(1, TimeUnit.MINUTES))
                    .build();

    private ExecutorService healthCheckExecutor = Executors.newFixedThreadPool(10);

    public Optional<WorkerBo> availableWorker(TaskBo task) {
        return WorkerSelector.builder()
                .withWorkers(ImmutableList.copyOf(workers.values()))
                .withLastFailedHosts(jobStateStore.getFailedHosts(task.getJobId()))
                .withTask(task)
                .select();
    }

    public List<WorkerBo> getWorkers() {
        return ImmutableList.copyOf(this.workers.values());
    }

    public void updateWorker(WorkerBo worker) {
        if (worker == null) return;

        workers.put(worker.getHost(), schedulerDao.saveNode(worker));
    }

    public WorkerBo shutdownWorker(String workerHost) {
        return setWorkerState(workerHost, SHUTDOWN);
    }

    public WorkerBo resumeWorker(String workerHost) {
        return setWorkerState(workerHost, ACTIVE);
    }

    public void unHealthyWorker(String host) {
        final WorkerBo worker = setWorkerState(host, UN_HEALTHY);

        runAsync(
                () -> {
                    try {
                        if (healthCheckRetryer.call(() -> healthCheck(worker))) {
                            setWorkerState(host, ACTIVE);
                            log.info("Worker_{} recovery from an un healthy state", host);
                        }
                    } catch (Exception e) {
                        setWorkerState(host, DEAD);
                        log.info("Worker_{} is dead after a few times of retrying", host);
                    }
                },
                healthCheckExecutor);
    }

    private WorkerBo setWorkerState(String host, WorkerState state) {
        WorkerBo worker = workers.get(host);
        if (worker != null) {
            worker.setWorkerState(state);
            schedulerDao.saveNode(worker);
        }

        log.info("Worker_{} is {}.", host, state);

        return worker;
    }

    private Boolean healthCheck(final WorkerBo worker) throws Exception {
        requireNonNull(worker);

        try (final RpcClient workerRpcClient =
                RpcClient.create(worker.getHost(), worker.getPort())) {
            HealthCheckRequest request = HealthCheckRequest.newBuilder().setService("").build();

            HealthCheckResponse healthCheckResponse = workerRpcClient.healthCheck(request);

            if (healthCheckResponse.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
                return true;
            }
        }

        return false;
    }

    public void activeWorker(ZkWorker zkWorker) {
        WorkerBo worker =
                schedulerDao.saveNode(
                        new WorkerBo()
                                .setHost(zkWorker.getIp())
                                .setPort(zkWorker.getPort())
                                .setWorkerState(ACTIVE));

        WorkerBo workerBo = workers.putIfAbsent(worker.getHost(), worker);

        if (workerBo != null) {
            log.info(
                    "Worker_{} is transit from {} to active.",
                    zkWorker.toString(),
                    workerBo.getWorkerState());
            workerBo.setWorkerState(ACTIVE);
        }

        log.info("Worker_{} is added to resource manager.", zkWorker.toString());
    }

    public void removeWorker(ZkWorker zkWorker) {
        workers.remove(zkWorker.getIp());
        schedulerDao.updateWorkerDead(zkWorker.getIp(), zkWorker.getPort());

        log.info("Worker_{} is removed from resource manager.", zkWorker.toString());
    }

    public void decrease(String host) {
        WorkerBo worker = workers.get(host);
        if (worker == null) return;
        synchronized (worker) {
            worker.setRunningTasks(worker.getRunningTasks() + 1);
        }
    }
}
