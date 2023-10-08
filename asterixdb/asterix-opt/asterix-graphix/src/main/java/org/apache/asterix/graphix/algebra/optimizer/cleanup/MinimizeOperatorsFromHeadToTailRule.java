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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Minimize the number of operations we have to do per iteration by pushing operators either down (i.e. before we enter
 * the loop) or after (i.e. after we perform the loop). The latter is necessary to minimize the number of marker
 * handling operations. TODO (GLENN): Expand this rule to find and push-down loop invariants!
 */
public class MinimizeOperatorsFromHeadToTailRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.MARKER_SINK) {
            List<ILogicalOperator> operatorsInLoop = new ArrayList<>();
            List<ILogicalOperator> operatorsAfterLoop = new ArrayList<>();
            if (!gatherOpsToFixedPointOp(opRef, operatorsInLoop, operatorsAfterLoop)) {
                return false;
            }
            if (!operatorsAfterLoop.isEmpty()) {
                rearrangeOpsToFixedPoint(opRef, operatorsInLoop, operatorsAfterLoop);
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
                return true;
            }
        }
        return false;
    }

    private boolean gatherOpsToFixedPointOp(Mutable<ILogicalOperator> opRef, List<ILogicalOperator> opsInLoop,
            List<ILogicalOperator> opsAfterLoop) {
        ILogicalOperator workingOp = opRef.getValue();
        switch (workingOp.getOperatorTag()) {
            case FIXED_POINT:
                // Base case: we have reached our fixed point operator.
                opsInLoop.add(opRef.getValue());
                return true;

            case MARKER_SINK:
            case RECURSIVE_HEAD:
                opsInLoop.add(opRef.getValue());
                break;

            case DISTINCT:
                DistinctOperator distinctOp = (DistinctOperator) workingOp;
                if (distinctOp instanceof PathSemanticsReductionOperator) {
                    opsInLoop.add(distinctOp);
                    break;
                }
                return false;

            case PROJECT:
            case SELECT:
            case ASSIGN:
                opsAfterLoop.add(opRef.getValue());
                break;

            default:
                // TODO (GLENN): Handle more operators in the future (we only expect these to be pushed down though).
                return false;
        }

        // Recurse until we reach our fixed-point operator.
        return gatherOpsToFixedPointOp(workingOp.getInputs().get(0), opsInLoop, opsAfterLoop);
    }

    private void rearrangeOpsToFixedPoint(Mutable<ILogicalOperator> opRef, List<ILogicalOperator> opsInLoop,
            List<ILogicalOperator> opsAfterLoop) {
        Iterator<ILogicalOperator> afterLoopIterator = opsAfterLoop.iterator();
        Iterator<ILogicalOperator> inLoopIterator = opsInLoop.iterator();

        // First, rearrange our plan and connect all after-loop-operators.
        ILogicalOperator previousAfterOp = afterLoopIterator.next();
        while (afterLoopIterator.hasNext()) {
            ILogicalOperator afterLoopOp = afterLoopIterator.next();
            previousAfterOp.getInputs().clear();
            previousAfterOp.getInputs().add(new MutableObject<>(afterLoopOp));
            previousAfterOp = afterLoopOp;
        }

        // Next, connect the last after-loop-operator to our top-most operator in the loop.
        ILogicalOperator previousInOp = inLoopIterator.next();
        previousAfterOp.getInputs().clear();
        previousAfterOp.getInputs().add(new MutableObject<>(previousInOp));
        while (inLoopIterator.hasNext()) {
            ILogicalOperator inLoopOp = inLoopIterator.next();
            previousInOp.getInputs().clear();
            previousInOp.getInputs().add(new MutableObject<>(inLoopOp));
            previousInOp = inLoopOp;
        }

        // Finally, set our operator reference.
        opRef.setValue(opsAfterLoop.get(0));
    }
}
