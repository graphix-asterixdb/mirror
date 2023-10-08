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
import java.util.OptionalInt;
import java.util.stream.IntStream;

import org.apache.asterix.graphix.algebra.annotations.JoinInLoopOperatorAnnotations;
import org.apache.asterix.graphix.algebra.operator.physical.PersistentBuildJoinPOperator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Given a plan with an {@link InnerJoinOperator} in the nested plan of a {@link FixedPointOperator}, finalize the
 * ordering of any {@link InnerJoinOperator}s (if they exist).
 * <ol>
 *   <li>If there is a {@link InnerJoinOperator} that is annotated with
 *   {@link JoinInLoopOperatorAnnotations#markAsJoinInLoop(InnerJoinOperator)}, ensure that this operator is the
 *   top-most {@link InnerJoinOperator} in a {@link FixedPointOperator}'s nested plan. We want to minimize the number
 *   of {@link PersistentBuildJoinPOperator} operators, which are more expensive than normal
 *   {@link HybridHashJoinPOperator}s.</li>
 *   <li>If there are no {@link InnerJoinOperator}s annotated, but we still have {@link InnerJoinOperator} in the
 *   {@link FixedPointOperator}'s nested plan, annotate all {@link InnerJoinOperator}s in a left tree traversal.</li>
 * </ol>
 * This rule should fire after the access method collection ({@link TransformJoinInLoopToINLJRule}).
 */
public class FinalizeJoinOperatorTreeInLoopRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
        Mutable<ILogicalOperator> rootRef = fpOp.getNestedPlans().get(0).getRoots().get(0);

        // Walk our plan in the loop. First, gather all JOIN operator references (BFS).
        List<Mutable<ILogicalOperator>> joinOpRefList = new ArrayList<>();
        Deque<Mutable<ILogicalOperator>> operatorRefQueue = new ArrayDeque<>();
        operatorRefQueue.add(rootRef);
        while (!operatorRefQueue.isEmpty()) {
            Mutable<ILogicalOperator> workingOpRef = operatorRefQueue.removeFirst();
            if (workingOpRef.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                joinOpRefList.add(workingOpRef);
            }
            for (Mutable<ILogicalOperator> inputOpRef : workingOpRef.getValue().getInputs()) {
                operatorRefQueue.addLast(inputOpRef);
            }
        }

        // If there are no JOINs...
        if (joinOpRefList.isEmpty()) {
            return false;
        }

        // ...or if our first JOIN is the annotated one, exit here.
        OptionalInt indexOfAnnotatedOp = IntStream.range(0, joinOpRefList.size()).filter(i -> {
            InnerJoinOperator joinOp = (InnerJoinOperator) joinOpRefList.get(i).getValue();
            return JoinInLoopOperatorAnnotations.isJoinInLoop(joinOp);
        }).findFirst();
        if (indexOfAnnotatedOp.isPresent() && indexOfAnnotatedOp.getAsInt() == 0) {
            return false;
        }

        // We still have JOINs, but none that are annotated. Walk the left inputs of our JOINs down to the leaves.
        if (indexOfAnnotatedOp.isEmpty()) {
            Deque<ILogicalOperator> operatorStack = new ArrayDeque<>();
            operatorStack.addLast(joinOpRefList.get(0).getValue());
            while (!operatorStack.isEmpty()) {
                ILogicalOperator workingOp = operatorStack.removeLast();
                if (workingOp.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                    InnerJoinOperator workingJoinOp = (InnerJoinOperator) workingOp;
                    JoinInLoopOperatorAnnotations.markAsJoinInLoop(workingJoinOp);
                    operatorStack.addLast(workingJoinOp.getInputs().get(0).getValue());

                } else {
                    for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                        operatorStack.addLast(inputRef.getValue());
                    }
                }
            }
            return false;
        }

        // We found an annotated JOIN. Swap our annotated JOIN operator with our uppermost JOIN operator.
        Mutable<ILogicalOperator> annotatedJoinOpRef = joinOpRefList.get(indexOfAnnotatedOp.getAsInt());
        Mutable<ILogicalOperator> persistentInputOpRef = annotatedJoinOpRef.getValue().getInputs()
                .get(PromoteSelectInLoopToJoinRule.JOIN_PERSISTENT_INPUT_INDEX);
        ILogicalOperator transientInputOp = persistentInputOpRef.getValue();
        ILogicalOperator oldTopJoinOp = joinOpRefList.get(0).getValue();
        joinOpRefList.get(0).setValue(annotatedJoinOpRef.getValue());
        persistentInputOpRef.setValue(oldTopJoinOp);
        annotatedJoinOpRef.setValue(transientInputOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rootRef.getValue(), context);
        return true;
    }
}
