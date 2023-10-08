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
package org.apache.asterix.graphix.algebra.optimizer.cleanup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveUnusedSubplanInFixedPointRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
        boolean isTransformed = false;

        // Walk our plan. We are looking for subplan operators.
        List<LogicalVariable> usedVariables = new ArrayList<>();
        Deque<Mutable<ILogicalOperator>> opRefStack = new ArrayDeque<>();
        opRefStack.addLast(fpOp.getNestedPlans().get(0).getRoots().get(0));
        while (!opRefStack.isEmpty()) {
            Mutable<ILogicalOperator> workingOpRef = opRefStack.removeLast();
            if (workingOpRef.getValue().getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                SubplanOperator subplanOp = (SubplanOperator) workingOpRef.getValue();
                List<LogicalVariable> producedVariables = new ArrayList<>();
                VariableUtilities.getProducedVariables(subplanOp, producedVariables);
                if (producedVariables.stream().noneMatch(usedVariables::contains)) {
                    isTransformed = true;
                    workingOpRef.setValue(subplanOp.getInputs().get(0).getValue());
                }
            }
            VariableUtilities.getUsedVariables(workingOpRef.getValue(), usedVariables);
            for (Mutable<ILogicalOperator> inputRef : workingOpRef.getValue().getInputs()) {
                opRefStack.addLast(inputRef);
            }
        }
        if (isTransformed) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(fpOp, context);
        }
        return isTransformed;
    }
}
