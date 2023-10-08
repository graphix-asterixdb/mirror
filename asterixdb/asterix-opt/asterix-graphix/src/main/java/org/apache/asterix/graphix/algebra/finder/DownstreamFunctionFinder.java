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
package org.apache.asterix.graphix.algebra.finder;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;

public class DownstreamFunctionFinder implements IAlgebraicEntityFinder<ILogicalOperator, Mutable<ILogicalExpression>> {
    private final Set<FunctionIdentifier> functionIdentifiers = new LinkedHashSet<>();

    private Mutable<ILogicalExpression> matchingExprRef;
    private ILogicalOperator containingOp;

    public DownstreamFunctionFinder(FunctionIdentifier functionIdentifier) {
        this.functionIdentifiers.add(functionIdentifier);
    }

    public DownstreamFunctionFinder(FunctionIdentifier... functionIdentifiers) {
        this.functionIdentifiers.addAll(Arrays.asList(functionIdentifiers));
    }

    @Override
    public boolean search(ILogicalOperator workingOp) throws AlgebricksException {
        Deque<ILogicalOperator> operatorQueue = new ArrayDeque<>(List.of(workingOp));

        // We'll use BFS here.
        while (!operatorQueue.isEmpty()) {
            ILogicalOperator headOp = operatorQueue.removeFirst();
            InExpressionFunctionFinder callFinder = new InExpressionFunctionFinder(functionIdentifiers);
            if (headOp.acceptExpressionTransform(callFinder::search)) {
                matchingExprRef = callFinder.get();
                containingOp = headOp;
                return true;
            }
            for (Mutable<ILogicalOperator> inputRef : headOp.getInputs()) {
                operatorQueue.addLast(inputRef.getValue());
            }
            if (headOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                // We will also move through NTS operators.
                NestedTupleSourceOperator ntsOp = (NestedTupleSourceOperator) headOp;
                operatorQueue.addLast(ntsOp.getSourceOperator());
            }
        }
        return false;
    }

    @Override
    public Mutable<ILogicalExpression> get() throws AlgebricksException {
        return matchingExprRef;
    }

    public ILogicalOperator getContainingOp() {
        return containingOp;
    }
}
