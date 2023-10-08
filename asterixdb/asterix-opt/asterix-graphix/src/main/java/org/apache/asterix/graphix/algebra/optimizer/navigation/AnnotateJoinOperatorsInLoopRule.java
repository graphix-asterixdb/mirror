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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.graphix.algebra.annotations.JoinInLoopOperatorAnnotations;
import org.apache.asterix.graphix.algebra.finder.ProducerOperatorFinder;
import org.apache.asterix.graphix.algebra.finder.VariableInReductionFinder;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class AnnotateJoinOperatorsInLoopRule implements IAlgebraicRewriteRule {
    private final Set<Collection<LogicalVariable>> previousVariableSet = new LinkedHashSet<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        switch (opRef.getValue().getOperatorTag()) {
            case DISTRIBUTE_RESULT:
            case DELEGATE_OPERATOR:
            case SINK:
                // We are at our root. Reset our state.
                previousVariableSet.clear();
                break;

            case RECURSIVE_TAIL:
                RecursiveTailOperator rsOp = (RecursiveTailOperator) opRef.getValue();
                previousVariableSet.add(rsOp.getPreviousIterationVariables());
                break;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        InnerJoinOperator joinOp = (InnerJoinOperator) op;

        // If our JOIN is not annotated...
        if (JoinInLoopOperatorAnnotations.isJoinInLoop(joinOp)) {
            return false;
        }

        // ... and our JOIN is dependent on previous variables...
        List<LogicalVariable> usedVariablesInOp = new ArrayList<>();
        VariableUtilities.getUsedVariables(joinOp, usedVariablesInOp);
        for (LogicalVariable usedVariable : usedVariablesInOp) {
            ProducerOperatorFinder opFinder = new ProducerOperatorFinder(joinOp);
            if (!opFinder.search(usedVariable)) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Producer operator not found?");
            }
            PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(opFinder.get(), context);
            List<FunctionalDependency> fdList = context.getFDList(opFinder.get());
            VariableInReductionFinder variableInReductionFinder = new VariableInReductionFinder(fdList, usedVariable);
            for (Iterator<Collection<LogicalVariable>> it = previousVariableSet.iterator(); it.hasNext();) {
                Collection<LogicalVariable> workingPreviousVarSet = it.next();
                if (workingPreviousVarSet.stream().anyMatch(variableInReductionFinder::search)) {
                    // ...then annotate this JOIN.
                    JoinInLoopOperatorAnnotations.markAsJoinInLoop(joinOp);
                    it.remove();
                    return true;

                }
            }
        }
        return false;
    }
}
