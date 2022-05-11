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

import io.github.artiship.allo.model.ha.ZkWorker;
import io.github.artiship.allo.scheduler.core.ResourceManager;
import io.github.artiship.allo.common.Service;
import io.github.artiship.allo.scheduler.rest.SchedulerBackend;
import io.github.com.artiship.ha.utils.CuratorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Iterables.getLast;
import static io.github.com.artiship.ha.GlobalConstants.DEAD_WORKER_GROUP;
import static io.github.com.artiship.ha.GlobalConstants.WORKER_GROUP;

@Slf4j
@Component
public class ZkWorkerListener implements PathChildrenCacheListener, Service {

    @Autowired
    private ResourceManager resourceManager;
    @Autowired
    private SchedulerBackend schedulerBackend;
    @Resource
    private CuratorFramework zkClient;
    private PathChildrenCache workers;

    @Override
    public void start() throws Exception {
        workers = new PathChildrenCache(zkClient, CuratorUtils.createPath(zkClient, WORKER_GROUP), true);
        workers.start();
        workers.getListenable()
               .addListener(this);
    }

    @Override
    public void stop() throws Exception {
        workers.close();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        String path = event.getData()
                           .getPath();
        try {
            String ipAndPort = getLast(on("/").split(path));
            ZkWorker zkWorker = ZkWorker.from(ipAndPort);

            switch (event.getType()) {
                case CHILD_ADDED:
                    log.info("Worker added : {}", path);
                    resourceManager.activeWorker(zkWorker);
                    break;
                case CHILD_REMOVED:
                    log.info("Worker removed : {}", path);

                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        log.warn("Worker remove sleep was interrupted", e);
                    }

                    if (client.checkExists().forPath(path) == null) {
                        resourceManager.removeWorker(zkWorker);
                        schedulerBackend.failoverTasks(zkWorker.getIp());
                        CuratorUtils.createPath(zkClient, DEAD_WORKER_GROUP + "/" + zkWorker.toString());
                    }
                    break;
                case CHILD_UPDATED:
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Zk worker listener for {} causes exception", path, e);
        }
    }
}
