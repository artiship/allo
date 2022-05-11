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

package io.github.artiship.allo.tra;

import io.github.artiship.allo.common.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Slf4j
public class CustomizedDependencyAnalyzerTest {

    @Test
    public void testParents() {
        DependencyAnalyzer analyzer =
                new CustomizedDependencyAnalyzer(
                        "0 2 3 * * ?",
                        "(yyyy-MM-dd 00:00:00, d(-2), d(0))",
                        TimeUtils.fromStr("2022-05-05 19:54:00"));

        assertEquals(
                analyzer.parents(),
                Arrays.asList(
                        TimeUtils.fromStr("2022-05-03 03:02:00"),
                        TimeUtils.fromStr("2022-05-04 03:02:00")));
    }
}
