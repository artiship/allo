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

package io.github.artiship.allo.worker.ha;

import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.model.ha.ZkWorker;
import io.github.artiship.allo.rpc.OsUtils;
import io.github.artiship.allo.worker.common.CommonCache;
import io.github.com.artiship.ha.utils.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static io.github.com.artiship.ha.GlobalConstants.LOST_TASK_GROUP;

@Slf4j
@Component
public class SuicideReactor implements PathChildrenCacheListener, Service {
    @Resource
    private CuratorFramework zkClient;
    private PathChildrenCache deadWorkers;

    @Override
    public void start() throws Exception {
        deadWorkers =
                new PathChildrenCache(
                        zkClient, CuratorUtils.createPath(zkClient, LOST_TASK_GROUP), true);
        deadWorkers.start();
        deadWorkers.getListenable().addListener(this);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String path = event.getData().getPath();

        switch (event.getType()) {
            case CHILD_ADDED:
                log.info("Dead worker added : {}", path);
                ZkWorker worker = ZkWorker.from(zkClient.getData().forPath(path));
                if (isSelf(worker)) {
                    suicide(worker);
                }
                break;
            default:
                break;
        }
    }

    private void suicide(ZkWorker worker) throws Exception {
        List<TaskBo> runningTasks = CommonCache.cloneRunningTask();
        if(runningTasks.size() != 0) {
            runningTasks.forEach((task) -> {
                log.info("Need to kill task [{}]", task);
                try {
                    //TODO kill
//                    CommonUtils.killTask(zkClientHolder, rpcClientHolder, task, taskState);
                } catch (Exception e) {
                    log.error(String.format("Task_%s_%s, kill task failed.", task.getJobId(), task.getId()), e);
                }
            });
        }
        //remove from zk
        CuratorUtils.unRegisterWorker(zkClient, worker);
    }

    private boolean isSelf(ZkWorker worker) {
        return OsUtils.getHostIpAddress().equals(worker.getIp());
    }

    @Override
    public void stop() throws Exception {

    }
}
