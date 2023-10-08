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
package org.apache.asterix.graphix.algebra.optimizer.navigation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Wrapper rule to use {@link IntroduceJoinAccessMethodRule} in the nested-plan(s) of a {@link FixedPointOperator}.
 */
public class TransformJoinInLoopToINLJRule implements IAlgebraicRewriteRule {
    private final IntroduceJoinAccessMethodRule inljRewriteRule = new IntroduceJoinAccessMethodRule() {
        @Override
        public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
                throws AlgebricksException {
            AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
            if (context.checkIfInDontApplySet(this, op)) {
                return false;
            }

            // Reset our state.
            clear();
            setMetadataDeclarations(context);
            afterJoinRefs = new ArrayList<>();

            // The meat: apply the JOIN transformation if applicable.
            if (checkAndApplyJoinTransformation(opRef, context, false)) {
                context.addToDontApplySet(this, joinOp);

                // If our JOIN transformation has been applied, remove any ORDER operators that have been introduced.
                Deque<Mutable<ILogicalOperator>> opRefStack = new ArrayDeque<>(List.of(opRef));
                while (!opRefStack.isEmpty()) {
                    Mutable<ILogicalOperator> workingOpRef = opRefStack.removeFirst();
                    if (workingOpRef.getValue().getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                        UnnestMapOperator searchOp = (UnnestMapOperator) workingOpRef.getValue();
                        Mutable<ILogicalOperator> searchInputOpRef = searchOp.getInputs().get(0);
                        if (searchInputOpRef.getValue().getOperatorTag() == LogicalOperatorTag.ORDER) {
                            // We have found an ORDER operator in the loop. Replace this reference with its input.
                            ILogicalOperator orderInputOp = searchInputOpRef.getValue().getInputs().get(0).getValue();
                            searchInputOpRef.setValue(orderInputOp);
                        }
                    }
                    for (Mutable<ILogicalOperator> inputRef : workingOpRef.getValue().getInputs()) {
                        opRefStack.addLast(inputRef);
                    }
                }

                // Recompute our types :-)
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
                return true;
            }
            return false;
        }
    };

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) op;

        // Invoke the INLJ rewrite rule for each nested plan.
        boolean isModified = false;
        for (ILogicalPlan planInLoop : fpOp.getNestedPlans()) {
            isModified |= inljRewriteRule.rewritePre(planInLoop.getRoots().get(0), context);
        }
        return isModified;
    }
}
