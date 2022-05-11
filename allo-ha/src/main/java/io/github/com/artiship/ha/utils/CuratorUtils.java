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

package io.github.com.artiship.ha.utils;

import com.google.common.base.Throwables;
import io.github.artiship.allo.model.ha.ZkScheduler;
import io.github.artiship.allo.model.ha.ZkWorker;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static io.github.com.artiship.ha.GlobalConstants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class CuratorUtils {
    private static final int ZK_CONNECTION_TIMEOUT_MILLIS = 15000;
    private static final int ZK_SESSION_TIMEOUT_MILLIS = 60000;
    private static final int RETRY_WAIT_MILLIS = 5000;
    private static final int MAX_RECONNECT_ATTEMPTS = 3;

    public static CuratorFramework newClient(String zkUrl) {
        final CuratorFramework client =
                CuratorFrameworkFactory.newClient(
                        zkUrl,
                        ZK_SESSION_TIMEOUT_MILLIS,
                        ZK_CONNECTION_TIMEOUT_MILLIS,
                        new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS));
        client.start();
        return client;
    }

    public static ZkScheduler activeMaster(CuratorFramework zk) throws Exception {
        return ZkScheduler.from(new String(zk.getData().forPath(SCHEDULER_GROUP), UTF_8));
    }

    public static void registerWorker(CuratorFramework zkClient, String host, Integer port)
            throws Exception {
        createPath(zkClient, getWorkerPath(host, port));
    }

    public static void unRegisterWorker(CuratorFramework zkClient, ZkWorker zkWorker) throws Exception {
        unRegisterWorker(zkClient, zkWorker.getIp(), zkWorker.getPort());
    }

    public static void unRegisterWorker(CuratorFramework zkClient, String host, Integer port)
            throws Exception {
        deletePath(zkClient, getWorkerPath(host, port));
    }

    private static String getWorkerPath(String host, Integer port) {
        return new StringBuilder()
                .append(WORKER_GROUP)
                .append(ZK_PATH_SEPARATOR)
                .append(host)
                .append(":")
                .append(port)
                .toString();
    }

    public static String deletePath(CuratorFramework zkClient, String path) throws Exception {
        if (zkClient.checkExists().forPath(path) != null) {
            try {
                zkClient.delete().forPath(path);
            } catch (Exception e) {
                log.warn("Delete Zk path {} fail.", path, e);
                Throwables.throwIfUnchecked(e);
            }
        }
        return path;
    }

    public static String createPath(CuratorFramework zkClient, String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(path);
            } catch (Exception e) {
                log.error("Zk path {} exists", path, e);
                Throwables.throwIfUnchecked(e);
            }
        }
        return path;
    }

    public static String createPath(CuratorFramework zkClient, String path, byte[] data)
            throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(path, data);
            } catch (Exception e) {
                log.error("Zk path {} exists", path, e);
                Throwables.throwIfUnchecked(e);
            }
        }
        return path;
    }
}
