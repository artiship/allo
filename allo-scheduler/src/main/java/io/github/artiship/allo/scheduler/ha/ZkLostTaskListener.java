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

package io.github.artiship.allo.scheduler.ha;

import io.github.artiship.allo.model.exception.TaskNotFoundException;
import io.github.artiship.allo.model.ha.ZkLostTask;
import io.github.artiship.allo.model.Service;
import io.github.artiship.allo.scheduler.rest.SchedulerBackend;
import io.github.com.artiship.ha.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static io.github.artiship.allo.model.GlobalConstants.LOST_TASK_GROUP;
import static java.util.concurrent.CompletableFuture.runAsync;

@Slf4j
@Component
public class ZkLostTaskListener implements PathChildrenCacheListener, Service {

    @Autowired private SchedulerBackend schedulerBackend;
    @Resource private CuratorFramework zkClient;
    private PathChildrenCache lostTasks;

    @Override
    public void start() throws Exception {
        lostTasks =
                new PathChildrenCache(
                        zkClient, CuratorUtils.createPath(zkClient, LOST_TASK_GROUP), true);
        lostTasks.start();
        lostTasks.getListenable().addListener(this);
    }

    @Override
    public void stop() throws Exception {
        lostTasks.close();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String path = event.getData().getPath();

        switch (event.getType()) {
            case CHILD_ADDED:
                log.info("Lost task added : {}", path);
                this.handleLostTask(path);
                break;
            case CHILD_REMOVED:
                log.info("Lost task removed : {}", path);
                break;
            case CHILD_UPDATED:
            default:
                break;
        }
    }

    public void handleLostTask(String path) {
        runAsync(
                () -> {
                    try {
                        schedulerBackend.updateLostTask(
                                ZkLostTask.from(zkClient.getData().forPath(path)));
                        zkClient.delete().forPath(path);
                    } catch (TaskNotFoundException e) {
                        try {
                            zkClient.delete().forPath(path);
                        } catch (Exception ex) {
                            log.error("Remove lost task from zk {} fail", path, e);
                        }
                    } catch (Exception e) {
                        log.error("Remove lost task zk node {} fail", path, e);
                    }
                });
    }
}
