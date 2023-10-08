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
package org.apache.asterix.graphix.algebra.optimizer.correction;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Cancel a {@link SubplanOperator} wrapping an {@link AggregateOperator} followed by a {@link FixedPointOperator}. This
 * rule performs two specific operations:
 * <ol>
 *   <li>Remove the {@link SubplanOperator} containing a {@link FixedPointOperator}, and promotes the
 *   {@link FixedPointOperator} out of the nested plan.</li>
 *   <li>Remove the {@link AggregateOperator} within the {@link SubplanOperator} that "listifies" the
 *   {@link FixedPointOperator} output.</li>
 * </ol>
 */
public class CancelSubplanListifyForLoopRule implements IAlgebraicRewriteRule {
    private final Map<LogicalVariable, LogicalVariable> variableReplacementMap = new LinkedHashMap<>();
    private final ILogicalExpressionReferenceTransform variableReplaceTransform =
            new ILogicalExpressionReferenceTransform() {
                @Override
                public boolean transform(Mutable<ILogicalExpression> exprRef) {
                    ILogicalExpression expr = exprRef.getValue();
                    switch (expr.getExpressionTag()) {
                        case FUNCTION_CALL:
                            AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) expr;
                            boolean hasChanged = false;
                            for (Mutable<ILogicalExpression> argRef : funcCallExpr.getArguments()) {
                                hasChanged |= transform(argRef);
                            }
                            return hasChanged;

                        case VARIABLE:
                            VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                            LogicalVariable logicalVar = varExpr.getVariableReference();
                            if (variableReplacementMap.containsKey(logicalVar)) {
                                varExpr.setVariable(variableReplacementMap.get(logicalVar));
                                return true;
                            }
                    }
                    return false;
                }
            };

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // If we can replace a variable, then we have performed our cancellation at an earlier iteration.
        if (opRef.getValue().acceptExpressionTransform(variableReplaceTransform)) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
            return true;

        } else if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplanOp = (SubplanOperator) opRef.getValue();

        // This subplan should contain a single nested plan with a single root...
        if (subplanOp.getNestedPlans().size() != 1 || subplanOp.getNestedPlans().get(0).getRoots().size() != 1) {
            return false;
        }
        Mutable<ILogicalOperator> rootOpRef = subplanOp.getNestedPlans().get(0).getRoots().get(0);

        // ...where the root is an aggregate operator that groups some $v in a list...
        if (rootOpRef.getValue().getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregateOp = (AggregateOperator) rootOpRef.getValue();
        List<Mutable<ILogicalExpression>> aggregateExprs = aggregateOp.getExpressions();
        if (aggregateExprs.size() != 1) {
            return false;
        }
        Mutable<ILogicalExpression> aggregateExprRef = aggregateOp.getExpressions().get(0);
        if (aggregateExprRef.getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression aggregateFuncRef = (AbstractFunctionCallExpression) aggregateExprRef.getValue();
        if (aggregateFuncRef.getFunctionIdentifier() != BuiltinFunctions.LISTIFY) {
            return false;
        }
        List<Mutable<ILogicalExpression>> aggregateFuncArgs = aggregateFuncRef.getArguments();
        if (aggregateFuncArgs.size() != 1) {
            return false;
        }
        ILogicalExpression aggregateFuncArgExpr = aggregateFuncArgs.get(0).getValue();
        if (aggregateFuncArgExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        // ...followed by a fixed-point operator.
        FixedPointOperator fpOp = null;
        Mutable<ILogicalOperator> bottomOpRef = null;
        Deque<Mutable<ILogicalOperator>> operatorRefQueue = new ArrayDeque<>();
        operatorRefQueue.addLast(aggregateOp.getInputs().get(0));
        while (!operatorRefQueue.isEmpty()) {
            Mutable<ILogicalOperator> workingOpRef = operatorRefQueue.removeFirst();
            switch (workingOpRef.getValue().getOperatorTag()) {
                case FIXED_POINT:
                    if (fpOp != null) {
                        return false;
                    }
                    fpOp = (FixedPointOperator) workingOpRef.getValue();
                    if (context.checkIfInDontApplySet(this, fpOp)) {
                        return false;
                    }
                    break;
                case NESTEDTUPLESOURCE:
                    if (bottomOpRef != null) {
                        return false;
                    }
                    bottomOpRef = workingOpRef;
                    break;
            }
            for (Mutable<ILogicalOperator> inputRef : workingOpRef.getValue().getInputs()) {
                operatorRefQueue.addLast(inputRef);
            }
        }
        if (fpOp == null || bottomOpRef == null) {
            return false;
        }
        ILogicalOperator aggregateInputOp = aggregateOp.getInputs().get(0).getValue();
        ILogicalOperator subplanInputOp = subplanOp.getInputs().get(0).getValue();
        variableReplacementMap.put(aggregateOp.getVariables().get(0),
                ((VariableReferenceExpression) aggregateFuncArgExpr).getVariableReference());

        // We have found a subplan to cancel and an aggregate to remove.
        Deque<AbstractLogicalOperator> operatorStack = new ArrayDeque<>();
        operatorStack.addLast((AbstractLogicalOperator) aggregateInputOp);
        while (!operatorStack.isEmpty()) {
            AbstractLogicalOperator workingOp = operatorStack.removeFirst();
            workingOp.setExecutionMode(subplanInputOp.getExecutionMode());
            if (workingOp.getOperatorTag() == LogicalOperatorTag.FIXED_POINT) {
                FixedPointOperator innerFpOp = (FixedPointOperator) workingOp;
                ILogicalOperator innerRootOp = innerFpOp.getNestedPlans().get(0).getRoots().get(0).getValue();
                operatorStack.addFirst((AbstractLogicalOperator) innerRootOp);
            }
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                operatorStack.addFirst((AbstractLogicalOperator) inputRef.getValue());
            }
        }
        bottomOpRef.setValue(subplanInputOp);
        opRef.setValue(aggregateInputOp);
        context.addToDontApplySet(this, fpOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
        return true;
    }
}
