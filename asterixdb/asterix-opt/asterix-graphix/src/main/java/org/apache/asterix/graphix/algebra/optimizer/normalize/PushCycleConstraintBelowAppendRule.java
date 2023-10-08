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
package org.apache.asterix.graphix.algebra.optimizer.normalize;

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.IS_DISTINCT_;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.graphix.algebra.finder.DownstreamFunctionFinder;
import org.apache.asterix.graphix.algebra.optimizer.navigation.TransformJoinInLoopToINLJRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * This rule is meant to fire before {@link TransformJoinInLoopToINLJRule} to enable full INLJ plans.
 */
public class PushCycleConstraintBelowAppendRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();

        // Find the cycle-prevention conjunct...
        Mutable<ILogicalOperator> rootRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
        DownstreamFunctionFinder cycleConstraintFinder = new DownstreamFunctionFinder(IS_DISTINCT_);
        if (!cycleConstraintFinder.search(rootRef.getValue())) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "No cycle constraint function found!");
        }
        Mutable<ILogicalExpression> cycleConstraintCallRef = cycleConstraintFinder.get();

        // ...and our APPEND_TO_PATH call.
        DownstreamFunctionFinder appendToPathFinder = new DownstreamFunctionFinder(APPEND_TO_EXISTING_PATH);
        if (!appendToPathFinder.search(rootRef.getValue())) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "No APPEND_TO_PATH call found!");
        }

        // We should only be working with JOINs or SELECTs.
        SelectOperator cyclePreventSelectOp;
        switch (cycleConstraintFinder.getContainingOp().getOperatorTag()) {
            case INNERJOIN:
                // We will generate a new SELECT for this if this is in a JOIN.
                break;

            case SELECT:
                // If this is a sole SELECT underneath our APPEND_TO_PATH op, then exit here.
                SelectOperator containingOp = (SelectOperator) cycleConstraintFinder.getContainingOp();
                List<Mutable<ILogicalExpression>> conditionConjuncts = new ArrayList<>();
                if (!containingOp.getCondition().getValue().splitIntoConjuncts(conditionConjuncts)) {
                    ILogicalOperator appendToPathCallOp = appendToPathFinder.getContainingOp();
                    if (appendToPathCallOp.getInputs().get(0).getValue().equals(containingOp)) {
                        return false;
                    }
                }
                break;

            default:
                // TODO (GLENN): Should we raise an exception here?
                return false;
        }
        cyclePreventSelectOp = new SelectOperator(new MutableObject<>(cycleConstraintCallRef.getValue()));
        cyclePreventSelectOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        cycleConstraintCallRef.setValue(ConstantExpression.TRUE);

        // We want to place this immediately below the call to APPEND_TO_PATH.
        Mutable<ILogicalOperator> inputRef = appendToPathFinder.getContainingOp().getInputs().get(0);
        cyclePreventSelectOp.getInputs().add(new MutableObject<>(inputRef.getValue()));
        inputRef.setValue(cyclePreventSelectOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rootRef.getValue(), context);
        return true;
    }
}
