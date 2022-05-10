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

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

@Slf4j
@Aspect
@Component
public class ServiceLogAop {

    @Around("execution(* io.github.artiship.allo.model.Service.start())")
    public void serviceStart(ProceedingJoinPoint joinPoint) throws Throwable {
        String serviceName = getServiceName(joinPoint);
        log.info("{} - Starting...", serviceName);
        joinPoint.proceed();
        log.info("{} - Start completed.", serviceName);
    }

    @Around("execution(* io.github.artiship.allo.model.Service.stop())")
    public void serviceStop(ProceedingJoinPoint joinPoint) throws Throwable {
        String serviceName = getServiceName(joinPoint);
        log.info("{} - Stopping...", serviceName);
        joinPoint.proceed();
        log.info("{} - Stop completed.", serviceName);
    }

    public String getServiceName(ProceedingJoinPoint joinPoint) {
        String serviceName = joinPoint.getTarget()
                                     .getClass()
                                     .getSimpleName();

        return LOWER_CAMEL.to(LOWER_HYPHEN, serviceName);
    }
}
