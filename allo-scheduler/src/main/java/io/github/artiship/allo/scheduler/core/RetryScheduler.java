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
import io.github.artiship.allo.scheduler.rpc.SchedulerRpcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.*;

import static io.github.artiship.allo.model.enums.TaskState.RETRYING;
import static io.github.artiship.allo.model.enums.TaskTriggerType.AUTO_RETRY;
import static java.time.Duration.between;
import static java.time.LocalDateTime.now;

@Slf4j
@Component
public class RetryScheduler extends AbstractScheduler {

    @Resource
    private SchedulerDao schedulerDao;
    @Resource
    private TaskDispatcher taskDispatcher;
    @Autowired
    private SchedulerRpcService schedulerRpcService;

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private Map<Long, ScheduledFuture<TaskBo>> retryingTasks = new ConcurrentHashMap<>();

    @Override
    public TaskBo submit(TaskBo task) {
        if (LocalDateTime.now().isAfter(task.nextRetryTime())) {
            return dispatch(task);
        }

        long waitDuration = between(now(), task.nextRetryTime()).getSeconds();
        ScheduledFuture<TaskBo> future = scheduledExecutorService.schedule(() -> dispatch(task),
                waitDuration,
                TimeUnit.SECONDS);

        log.info("{} RETRY scheduled: retry time={}, left={}", task.traceId(), task.nextRetryTime(), waitDuration);

        retryingTasks.put(task.getId(), future);

        return task;
    }

    @Override
    public void start() throws Exception {
        schedulerRpcService.registerListener(this);
        log.info("RetryScheduler restore start...");
        this.schedulerDao.getTasksByState(RETRYING)
                         .forEach(t -> {
                             log.info("{} {} RELOAD: retry time={}", t.traceId(), t.getTaskState(), t.nextRetryTime());
                             submit(t);
                         });
        log.info("RetryScheduler restore end...");
    }

    @Override
    public void onFail(TaskBo task) {
        if (task.triggeredManually()) {
            return;
        }

        if (!task.retryable()) {
            return;
        }

        TaskBo submit = submit(schedulerDao.saveTask(task.toRenewTask()
                                                                  .setRetryTimes(task.getRetryTimes() + 1)
                                                                  .setTaskTriggerType(AUTO_RETRY)
                                                                  .setTaskState(RETRYING)));

        log.info("{} RETRY submitted: retry task={}, retry time={}", task.traceId(), submit.traceId(), submit.nextRetryTime());
    }

    private TaskBo dispatch(final TaskBo task) {
        TaskBo submit = taskDispatcher.submit(task);

        log.info("{} RETRY start: times={}/{}.", task.traceId(), submit.getRetryTimes(), submit.getMaxRetryTimes());

        if (retryingTasks.containsKey(task.getId())) {
            retryingTasks.remove(task.getId());
        }

        return submit;
    }

    @Override
    public void stop() throws Exception {
        if (this.scheduledExecutorService == null || this.scheduledExecutorService.isShutdown())
            return;

        this.scheduledExecutorService.shutdownNow();
    }

    public void kill(TaskBo task) {
        ScheduledFuture<TaskBo> future = retryingTasks.get(task.getId());
        if (future != null) {
            future.cancel(true);
        }
    }
}
