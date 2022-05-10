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

import io.github.artiship.allo.model.tra.DependencyExpr;
import io.github.artiship.allo.tra.parser.DependencyBaseVisitor;
import io.github.artiship.allo.tra.parser.DependencyParser;

import java.time.LocalDateTime;

public class DependencyExprVisitor extends DependencyBaseVisitor<DependencyExpr> {
    private final LocalDateTime childSchedule;

    public DependencyExprVisitor(LocalDateTime childSchedule) {
        this.childSchedule = childSchedule;
    }

    @Override
    public DependencyExpr visitExpr(DependencyParser.ExprContext ctx) {
        return DependencyExpr.of(
                new DependencyRuleVisitor().visitDependencyRule(ctx.dependencyRule()),
                new DependencyRangeVisitor(childSchedule)
                        .visitDependencyRange(ctx.dependencyRange()));
    }
}
