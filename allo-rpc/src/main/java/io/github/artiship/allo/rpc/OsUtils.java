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

package io.github.artiship.allo.rpc;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.math.BigDecimal;
import java.net.InetAddress;

public class OsUtils {
    
    private static SystemInfo systemInfo = new SystemInfo();

    public static GlobalMemory getMemory() {
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        return hal.getMemory();
    }

    public static CentralProcessor getProcessor() {
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        return hal.getProcessor();
    }

    public static double getMemoryTotal(GlobalMemory memory) {
        return memory.getTotal();
    }

    public static double getMemoryAvailable(GlobalMemory memory) {
        return memory.getAvailable();
    }

    public static double getMemoryUsage(GlobalMemory memory) {
        double available = getMemoryAvailable(memory);
        double total = getMemoryTotal(memory);
        BigDecimal bg = new BigDecimal(100 -(available/total * 100));
        return bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static double getMemoryUsage() {
        return getMemoryUsage(getMemory());
    }

    public static double getCpuUsage() {
        CentralProcessor processor = getProcessor();
        double useRate = processor.getSystemCpuLoadBetweenTicks();
        BigDecimal bg = new BigDecimal(useRate * 100);
        return bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static String getHostName() {
        InetAddress address;
        String hostName;
        try{
            address = InetAddress.getLocalHost();
            hostName = address.getHostName(); //获得机器名称
        }catch(Exception e){  
            throw new RuntimeException("can not find host name"); 
        }  
        return hostName;
    }
    
    /**
     * 获取主机ip地址
     * @return
     */
    public static String getHostIpAddress() {
        InetAddress address;
        String ip;
        try{
            address = InetAddress.getLocalHost();
            ip = address.getHostAddress(); //获得机器IP　　
        }catch(Exception e){  
            throw new RuntimeException("can not find ip address");  
        } 
        return ip;
    }
}