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
import io.github.artiship.allo.model.tra.DependencyRange;
import io.github.artiship.allo.model.tra.DependencyRule;
import io.github.artiship.allo.model.tra.DependencyRuleType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DependencyExprParserTest {

    @Test
    public void testRange() {
        assertRange(
                "(yyyy-MM-dd HH:mm:ss, d(-1)h(-1), d(1)h(1)]",
                "2022-05-04 21:59:11",
                "2022-05-03 20:59:11",
                "2022-05-05 22:59:11");

        assertRange(
                "(yyyy-MM-dd 00:00:00, d(-1), d(1)]",
                "2022-05-04 21:59:11",
                "2022-05-03 00:00:00",
                "2022-05-05 00:00:00");
    }

    @Test
    public void testRule() {
        assertRule("*", DependencyRuleType.ALL, null);
        assertRule("A", DependencyRuleType.ANY, null);
        assertRule("F(1)", DependencyRuleType.FIRST, 1);
        assertRule("L(1)", DependencyRuleType.LAST, 1);
        assertRule("C(2)", DependencyRuleType.CONTINUES, 2);
    }

    private void assertRule(String expr, DependencyRuleType ruleType, Integer scalar) {
        DependencyRule dependencyRule = DependencyExprParser.parseRule(expr);
        assertThat(dependencyRule.getDependencyRuleType()).isEqualTo(ruleType);
        assertThat(dependencyRule.getScalar()).isEqualTo(scalar);
    }

    private void assertRange(String expr, String childTime, String parentStart, String parentEnd) {
        DependencyRange range = DependencyExprParser.parseRange(expr, TimeUtils.fromStr(childTime));

        assertThat(range.getRangeStart().getRangeTime()).isEqualTo(TimeUtils.fromStr(parentStart));
        assertThat(range.getRangEnd().getRangeTime()).isEqualTo(TimeUtils.fromStr(parentEnd));
    }
}
