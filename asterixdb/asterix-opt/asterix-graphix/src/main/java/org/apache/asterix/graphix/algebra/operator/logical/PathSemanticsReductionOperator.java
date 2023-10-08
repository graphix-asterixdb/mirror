/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.operator.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;

public class PathSemanticsReductionOperator extends DistinctOperator {
    private final List<Mutable<ILogicalExpression>> sourceDistinctExprList;
    private final List<Mutable<ILogicalExpression>> destDistinctExprList;
    private final Mutable<ILogicalExpression> weightExpr;
    private final long kValue;

    public PathSemanticsReductionOperator(List<Mutable<ILogicalExpression>> sourceDistinctExprList,
            List<Mutable<ILogicalExpression>> destDistinctExprList, Mutable<ILogicalExpression> weightExpr, long k) {
        super((weightExpr == null)
                ? Stream.concat(sourceDistinctExprList.stream(), destDistinctExprList.stream())
                        .collect(Collectors.toList())
                : Stream.concat(Stream.concat(sourceDistinctExprList.stream(), destDistinctExprList.stream()),
                        Stream.of(weightExpr)).collect(Collectors.toList()));
        this.sourceDistinctExprList = Objects.requireNonNull(sourceDistinctExprList);
        this.destDistinctExprList = Objects.requireNonNull(destDistinctExprList);
        this.weightExpr = weightExpr;
        if (k <= 0) {
            throw new IllegalArgumentException("K must be greater than 0!");
        }
        this.kValue = k;
    }

    public List<Mutable<ILogicalExpression>> getSourceDistinctExprList() {
        return Collections.unmodifiableList(sourceDistinctExprList);
    }

    public List<Mutable<ILogicalExpression>> getDestDistinctExprList() {
        return Collections.unmodifiableList(destDistinctExprList);
    }

    public Mutable<ILogicalExpression> getWeightExpr() {
        return weightExpr;
    }

    public long getKValue() {
        return kValue;
    }

    @Override
    public void recomputeSchema() {
        // Unlike the DISTINCT operator, we do not lead with our distinct expressions.
        schema = new ArrayList<>(inputs.get(0).getValue().getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }
}
