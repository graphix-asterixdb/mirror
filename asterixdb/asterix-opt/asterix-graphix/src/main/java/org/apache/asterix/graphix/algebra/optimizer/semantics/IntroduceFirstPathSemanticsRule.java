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
package org.apache.asterix.graphix.algebra.optimizer.semantics;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * Given a plan with a {@link FixedPointOperator}, we might be able to avoid enumerating all outputs if we can safely
 * reason that not all paths are required. If the following conditions are met, we will introduce a
 * {@link DistinctOperator} outside of our {@link FixedPointOperator}:
 * <ol>
 *   <li>A {@link FixedPointOperator} exists with {@code source}, {@code destination}, and {@code path}
 *   operator annotations...</li>
 *   <li>...and there exists a {@link DistinctOperator} or {@link GroupByOperator} downstream that operates on our
 *   {@code source} and/or our {@code destination} variables (or variables / expressions that are functionally
 *   dependent on these variables),
 *   <b>BUT</b> not our {@code path} variable.</li>
 * </ol>
 *
 * @apiNote This rule should run after {@link IntroduceCheapestPathSemanticsRule}.
 */
public class IntroduceFirstPathSemanticsRule extends AbstractIntroducePathSemanticsRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // If we haven't encountered any FIXED-POINT operators, we can exit early.
        if (contextSet.isEmpty()) {
            return false;
        }

        // Similarly, if we have already applied this transformation, exit early.
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // We are searching for DISTINCT and GROUP-BY operators.
        List<LogicalVariable> groupingVarList;
        switch (op.getOperatorTag()) {
            case DISTINCT:
                if (op instanceof PathSemanticsReductionOperator) {
                    return false;
                }
                DistinctOperator distinctOp = (DistinctOperator) op;
                groupingVarList = distinctOp.getDistinctByVarList();
                break;

            case GROUP:
                GroupByOperator groupByOp = (GroupByOperator) op;
                groupingVarList = groupByOp.getGroupByVarList();
                break;

            default:
                return false;
        }

        // Generate a function that tells us if a variable is dependent on this GROUP-BY / DISTINCT.
        Predicate<LogicalVariable> doesDetermineGrouping = createReductionPredicate(op, groupingVarList, context);

        // If there exists any GROUP-BY variable that is dependent on our path, then prune these FPs here.
        Set<FixedPointOperatorContext> survivingFPOps = contextSet.stream()
                .filter(fc -> !doesDetermineGrouping.test(fc.pathVariable)).collect(Collectors.toSet());

        // Iterate through our surviving FP operators.
        boolean isChanged = false;
        for (FixedPointOperatorContext fc : survivingFPOps) {
            boolean sourceDeterminesGrouping = fc.startingVariables.stream().allMatch(doesDetermineGrouping);
            boolean destDeterminesGrouping = fc.endingVariables.stream().allMatch(doesDetermineGrouping);
            boolean downDeterminesGrouping = fc.downVariables.stream().allMatch(doesDetermineGrouping);
            if (!sourceDeterminesGrouping || !(destDeterminesGrouping || downDeterminesGrouping)) {
                continue;
            }
            FixedPointOperator fpOp = (FixedPointOperator) fc.fpOpRef.getValue();

            // We have found a valid <DISTINCT/GROUP-BY, FIXED-POINT> operator pair.
            // TODO (GLENN): Extend this for ANY-K semantics (i.e. search for a LIMIT operator).
            PathSemanticsReductionOperator underFpDistinctOp =
                    createDistinctOp(fc, context, (s, d) -> new PathSemanticsReductionOperator(s, d, null, 1));
            Mutable<ILogicalOperator> rootOpRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
            ILogicalOperator previousRootOp = rootOpRef.getValue();
            underFpDistinctOp.getInputs().add(new MutableObject<>(previousRootOp));
            rootOpRef.setValue(underFpDistinctOp);
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(fpOp, context);
            context.addToDontApplySet(this, op);
            contextSet.remove(fc);
            isChanged = true;
        }
        return isChanged;
    }
}
