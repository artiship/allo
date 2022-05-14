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

import com.google.common.base.Strings;
import io.github.artiship.allo.common.TimeUtils;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.rpc.OsUtils;
import io.github.artiship.allo.storage.SharedStorage;
import io.github.com.artiship.ha.GlobalConstants;
import io.github.com.artiship.ha.TaskStateListener;
import io.github.com.artiship.ha.TaskStateNotifier;
import lombok.extern.slf4j.Slf4j;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.LogOutputStream;

import java.io.File;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.concurrent.Future;

@Slf4j
public class LocalExecutor implements AlloExecutor {
    private final TaskBo task;
    private final SharedStorage sharedStorage;
    private final String taskLocalBasePath;

    private final TaskStateNotifier taskStateNotifier = new TaskStateNotifier();
    private Future future;
    private Long pid;
    private String applicationId;

    public LocalExecutor(TaskBo task, SharedStorage sharedStorage, String taskLocalBasePath) {
        this.task = task;
        this.sharedStorage = sharedStorage;
        this.taskLocalBasePath = taskLocalBasePath;
    }

    public void register(TaskStateListener listener) {
        taskStateNotifier.registerListener(listener);
    }

    @Override
    public void execute() {
        try {
            taskStateNotifier.notifyRunning(task);

            sharedStorage.download(task.getJobStoragePath(), getTaskLocalPath());

            String runCommand = buildRunShellCommand();

            sharedStorage.appendLog(runCommand);
            sharedStorage.appendLog("worker=" + OsUtils.getHostIpAddress());

            StartedProcess startedProcess = new ProcessExecutor()
                    .command("/bin/bash", "-c", runCommand)
                    .redirectError(new LogHandler())
                    .redirectOutput(new LogHandler())
                    .destroyOnExit()
                    .start();

            try {
                pid = getLinuxPid(startedProcess.getProcess());
                task.setPid(pid);
                log.info("Task_{}, process pid {}.", task.traceId(), pid);

                taskStateNotifier.notifyRunning(task);
            } catch (Exception e) {
                log.warn("Task_{} get process id fail.", task.traceId());
            }

            future = startedProcess.getFuture();
            ProcessResult processResult = (ProcessResult)future.get();
            if (processResult.getExitValue() != 0) {
                taskStateNotifier.notifyFail(task);
            }
        } catch (Exception e) {
            taskStateNotifier.notifyFail(task);
        } finally {

        }
        taskStateNotifier.notifySuccess(task);
    }

    private Long getLinuxPid(Process process) throws Exception{
        long pid = -1;
        Class<?>  clazz = Class.forName("java.lang.UNIXProcess");
        Field field = clazz.getDeclaredField("pid");
        field.setAccessible(true);
        pid = (Integer) field.get(process);
        return pid;
    }

    class LogHandler extends LogOutputStream {
        public static final String SPARK_APPLICATION_ID_TAG = "Application Id:";
        public static final String HIVE_APPLICATION_ID_TAG = "Tracking URL =";
        public static final String APPLICATION_ID_TAG = "application_";

        @Override
        protected void processLine(String line) {
            try {
                sharedStorage.appendLog(line);
                applicationId = extractApplicationId(line);

                if (applicationId != null && applicationId.length() > 0) {
                    task.addApplicationId(applicationId);
                    taskStateNotifier.notifyRunning(task);
                }
            } catch (Exception e) {
                log.warn("Task_{} process log fail.", task.traceId());
            }
        }

        private String extractApplicationId(String log) {
            if(log.contains(SPARK_APPLICATION_ID_TAG)) {
                return getApplicationIdBySparkLog(log, SPARK_APPLICATION_ID_TAG);
            } else if(log.contains(HIVE_APPLICATION_ID_TAG)) {
                return getApplicationIdByHiveLog(log, APPLICATION_ID_TAG);
            }
            return "";
        }

        private String getApplicationIdBySparkLog(String runningLog, String applicationIdTag) {
            int startIndex = runningLog.indexOf(applicationIdTag);
            if(startIndex != -1) {
                return runningLog.substring(startIndex + applicationIdTag.length());
            }
            return "";
        }

        private String getApplicationIdByHiveLog(String runningLog, String applicationIdTag) {
            int startIndex = runningLog.indexOf(applicationIdTag);
            int endIndex = runningLog.length() - 1;

            if(startIndex!=-1 && startIndex <= endIndex) {
                return runningLog.substring(startIndex, endIndex);
            }

            return "";
        }
    }

    private String buildRunShellCommand() {
        return new StringBuilder()
                .append("cd ")
                .append(this.taskLocalBasePath)
                .append(";")
                .append("dos2unix *.sh;")
                .append("chmod 777 *.sh;")
                .append("sh ")
                .append(GlobalConstants.START_SHELL_FILE_NAME)
                .append(" ")
                .append(TimeUtils.toStr(task.getScheduleTime()))
                .append(" ")
                .append(task.getJobId())
                .append(" ")
                .append(task.getId())
                .toString();
    }

    private String getTaskLocalPath() {
        return new StringBuilder()
                .append(taskLocalBasePath)
                .append(File.separator)
                .append(TimeUtils.toStr(LocalDateTime.now()))
                .append(File.separator)
                .append(task.getJobId())
                .append(File.separator)
                .append(task.getId())
                .toString();
    }

    @Override
    public void kill() throws Exception {
        if (!Strings.isNullOrEmpty(applicationId)) {
            LinuxProcessUtils.killYarnApplicationById(applicationId);
        }

        if (pid != null) {
            LinuxProcessUtils.killLinuxProcessByPid(pid);
        }

        if (future != null) {
            future.cancel(true);
        }
    }
}
