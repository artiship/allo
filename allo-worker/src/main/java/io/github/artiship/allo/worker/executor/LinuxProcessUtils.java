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

package io.github.artiship.allo.worker.executor;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.LogOutputStream;
import org.zeroturnaround.process.PidProcess;
import org.zeroturnaround.process.ProcessUtil;
import org.zeroturnaround.process.Processes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class LinuxProcessUtils {
    public static final String GET_CHILD_PID =
            "ps -ef|awk '{print $2\" \"$3}'|grep \" %s\"|awk '{print $1}'";

    public static void killYarnApplicationById(String applicationId) throws Exception {
        StringBuilder commandBuilder =
                new StringBuilder("source /etc/profile;")
                        .append("export HADOOP_USER_NAME=hadoop;")
                        .append("yarn application -kill ")
                        .append(applicationId);

        StartedProcess startedProcess =
                new ProcessExecutor()
                        .command("/bin/bash", "-c", commandBuilder.toString())
                        .destroyOnExit()
                        .start();

        int exitCode = startedProcess.getFuture().get().getExitValue();
        if (0 != exitCode) {
            throw new RuntimeException(
                    String.format(
                            "kill yarn application failed. yarn application id [%s], exit code [%s]",
                            applicationId, exitCode));
        }
    }

    public static void killLinuxProcessByPid(Long pid) throws Exception {
        if (null != pid && -1 != pid) {
            List<String> pidTree = getPidTree(pid);
            Collections.sort(pidTree, Ordering.natural().nullsLast().reverse());

            log.info("Pid tree of {} is {}", pid, pidTree);
            for (String pidString : pidTree) {
                PidProcess process = Processes.newPidProcess(Integer.parseInt(pidString));
                if (process.isAlive()) {
                    destroyGracefullyOrForcefullyAndWait(pidString);

                    log.info("Kill Pid {}, ParentId {}", pidString, pid);
                }
            }
        }
    }

    public static void destroyGracefullyOrForcefullyAndWait(String pidString)
            throws InterruptedException, TimeoutException, IOException {
        int pid = Integer.parseInt(pidString);
        PidProcess process = Processes.newPidProcess(pid);
        ProcessUtil.destroyGracefullyOrForcefullyAndWait(process, 10, SECONDS, 5, SECONDS);
    }

    private static List<String> getPidTree(Long pid)
            throws InterruptedException, TimeoutException, IOException {
        List<String> needGetChildPids = Lists.newArrayList();
        needGetChildPids.add(pid.toString());

        List<String> removeHasGetChildPidsPid = Lists.newArrayList();
        List<String> newNeedGetChildPidsPid = Lists.newArrayList();
        List<String> allPids = Lists.newArrayList();

        while (needGetChildPids.size() != 0) {
            for (String needGetChildPid : needGetChildPids) {
                String getChildPidCommand = String.format(GET_CHILD_PID, needGetChildPid);
                ProcessExecutor processExecutor = new ProcessExecutor();
                processExecutor
                        .command("/bin/bash", "-c", getChildPidCommand)
                        .redirectOutput(
                                new LogOutputStream() {
                                    @Override
                                    protected void processLine(String childPid) {
                                        if (StringUtils.isNotBlank(childPid)
                                                && StringUtils.isNumeric(childPid)) {
                                            allPids.add(childPid);
                                            newNeedGetChildPidsPid.add(childPid);
                                        }
                                    }
                                })
                        .destroyOnExit()
                        .execute()
                        .getExitValue();

                removeHasGetChildPidsPid.add(needGetChildPid);
            }

            needGetChildPids.removeAll(removeHasGetChildPidsPid);
            needGetChildPids.addAll(newNeedGetChildPidsPid);
            removeHasGetChildPidsPid.clear();
            newNeedGetChildPidsPid.clear();
        }
        allPids.add(pid.toString());
        return allPids;
    }
}
