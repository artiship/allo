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

package io.github.artiship.allo.storage;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Slf4j
public class OssStorage implements SharedStorage {
    private static final String START_SHELL_FILE_NAME = "start.sh";

    private OSSClient ossClient;
    private OssConfig ossConfig;
    private String localBasePath;

    public OssStorage(OSSClient ossClient, OssConfig ossConfig, String localBasePath) {
        this.ossClient = ossClient;
        this.ossConfig = ossConfig;
        this.localBasePath = localBasePath;
    }

    @Override
    public void download(String sourcePath, String targetPath) {
        cleanDirectory(targetPath);

        listFiles(sourcePath)
                .ifPresent(
                        list ->
                                list.forEach(
                                        objectSummary -> {
                                            download(sourcePath, targetPath, objectSummary);
                                        }));

        if (!check(targetPath)) {
            throw new RuntimeException(START_SHELL_FILE_NAME + " does not found.");
        }
    }

    @Override
    public void appendLog(String log) {

    }

    public boolean check(String packageLocalPath) {
        File runPath = new File(packageLocalPath);
        if (runPath.exists()) {
            File startAllShellFile =
                    new File(packageLocalPath + File.separator + START_SHELL_FILE_NAME);
            if (startAllShellFile.exists()) {
                return true;
            }
        }

        return false;
    }

    private void cleanDirectory(String path) {
        try {
            File targetPath = new File(path);
            Files.deleteIfExists(targetPath.toPath());
            Files.createDirectory(targetPath.toPath());
        } catch (Exception e) {
            log.warn("Clean directory {} fail.", path, e);
        }
    }

    private void download(String sourcePath, String targetPath, OSSObjectSummary objectSummary) {
        String sourceFilePath = objectSummary.getKey();
        String filename = sourceFilePath.substring(sourceFilePath.lastIndexOf("/") + 1);

        if (filename.indexOf(".") == -1) return;

        downloadFile(sourceFilePath, targetPath + File.separator + filename);
    }

    private void downloadFile(String sourceFilePath, String targetFilePath) {
        ossClient.getObject(
                new GetObjectRequest(ossConfig.getBucket(), sourceFilePath),
                new File(targetFilePath));
    }

    private Optional<List<OSSObjectSummary>> listFiles(String commonPath) {
        ObjectListing objects =
                ossClient.listObjects(
                        new ListObjectsRequest(ossConfig.getBucket())
                                .withPrefix(commonPath)
                                .withMaxKeys(ossConfig.getListMaxKeys()));
        if (objects == null) {
            log.info("Oss common path {} does not exist", commonPath);
            return empty();
        }

        return of(objects.getObjectSummaries());
    }
}
