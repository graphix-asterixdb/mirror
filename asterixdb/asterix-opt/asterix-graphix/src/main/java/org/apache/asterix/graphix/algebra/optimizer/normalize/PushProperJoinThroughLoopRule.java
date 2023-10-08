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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushProperJoinThroughLoopRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        switch (opRef.getValue().getOperatorTag()) {
            case INNERJOIN:
                break;

            default:
                return false;
        }
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) opRef.getValue();
        ILogicalExpression joinCond = joinOp.getCondition().getValue();

        // We will not push a product down (similar to PushProperJoinThroughProduct).
        if (OperatorPropertiesUtil.isAlwaysTrueCond(joinCond)) {
            return false;
        }
        List<Mutable<ILogicalExpression>> joinConjunctRefs = new ArrayList<>();
        joinCond.splitIntoConjuncts(joinConjunctRefs);

        // We explore our right branch (the purpose of this rule is to allow INLJ).
        List<LogicalVariable> toFixedPointVariables = new ArrayList<>();
        Deque<ILogicalOperator> rightOpQueue = new ArrayDeque<>();
        rightOpQueue.add(joinOp.getInputs().get(1).getValue());
        FixedPointOperator fpOp = null;
        while (!rightOpQueue.isEmpty()) {
            ILogicalOperator workingOp = rightOpQueue.removeFirst();
            switch (workingOp.getOperatorTag()) {
                case MARKER_SINK:
                case RECURSIVE_HEAD:
                    break;

                case DISTINCT:
                    DistinctOperator distinctOp = (DistinctOperator) workingOp;
                    if (distinctOp instanceof PathSemanticsReductionOperator) {
                        break;
                    }
                    return false;

                case FIXED_POINT:
                    fpOp = (FixedPointOperator) workingOp;
                    break;

                default:
                    if (workingOp.isMap()) {
                        break;
                    }
                    return false;
            }

            // We have found our fixed-point.
            if (fpOp != null) {
                break;
            }

            // Explore our right JOIN branch further.
            VariableUtilities.getProducedVariables(workingOp, toFixedPointVariables);
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                rightOpQueue.addLast(inputRef.getValue());
            }
        }
        if (fpOp == null) {
            return false;
        }

        // We have found a fixed-point operator. Gather the live variables of our left branch and our FP input.
        List<LogicalVariable> leftBranchLiveVariables = new ArrayList<>();
        List<LogicalVariable> fixedPointInputVariables = new ArrayList<>();
        VariableUtilities.getLiveVariables(joinOp.getInputs().get(0).getValue(), leftBranchLiveVariables);
        VariableUtilities.getLiveVariables(fpOp.getInputs().get(0).getValue(), fixedPointInputVariables);

        // Does there exist a conjunct from our JOIN that can be pushed below our JOIN?
        List<Mutable<ILogicalExpression>> pushableConjunctRefs = new ArrayList<>();
        for (Mutable<ILogicalExpression> joinConjunctRef : joinConjunctRefs) {
            List<LogicalVariable> joinConjunctUsedVariables = new ArrayList<>();
            joinConjunctRef.getValue().getUsedVariables(joinConjunctUsedVariables);
            boolean foundInLeftBranch = joinConjunctUsedVariables.stream().anyMatch(leftBranchLiveVariables::contains);
            boolean foundInFPInput = joinConjunctUsedVariables.stream().anyMatch(fixedPointInputVariables::contains);
            if (foundInLeftBranch && foundInFPInput) {
                pushableConjunctRefs.add(joinConjunctRef);
            }
        }
        if (pushableConjunctRefs.isEmpty()) {
            return false;
        }

        // We have a conjunct we can push down. Handle the non-pushable conjuncts first...
        joinConjunctRefs.removeAll(pushableConjunctRefs);
        if (joinConjunctRefs.size() == 0) {
            // Case #1: push our JOIN input up.
            opRef.setValue(joinOp.getInputs().get(1).getValue());

        } else if (joinConjunctRefs.size() == 1) {
            // Case #2: replace our JOIN with a SELECT of one conjunct.
            SelectOperator selectOp = new SelectOperator(new MutableObject<>(joinConjunctRefs.get(0).getValue()));
            selectOp.setSourceLocation(joinOp.getSourceLocation());
            selectOp.getInputs().add(new MutableObject<>(joinOp.getInputs().get(1).getValue()));
            opRef.setValue(selectOp);

        } else { // joinConjunctRefs.size() > 1
            // Case #3: replace our JOIN with a SELECT of multiple conjuncts.
            AbstractFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                    joinConjunctRefs.stream().map(e -> new MutableObject<>(e.getValue())).collect(Collectors.toList()));
            SelectOperator selectOp = new SelectOperator(new MutableObject<>(andExpr));
            selectOp.setSourceLocation(joinOp.getSourceLocation());
            selectOp.getInputs().add(new MutableObject<>(joinOp.getInputs().get(1).getValue()));
            opRef.setValue(selectOp);
        }

        // ...and then our pushable conjuncts.
        Mutable<ILogicalExpression> newJoinCond;
        if (pushableConjunctRefs.size() == 1) {
            newJoinCond = new MutableObject<>(pushableConjunctRefs.get(0).getValue());

        } else { // pushableConjunctRefs.size() > 1
            AbstractFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), pushableConjunctRefs.stream()
                            .map(e -> new MutableObject<>(e.getValue())).collect(Collectors.toList()));
            newJoinCond = new MutableObject<>(andExpr);
        }
        InnerJoinOperator newJoinOp = new InnerJoinOperator(newJoinCond);
        ILogicalOperator fpInputOp = fpOp.getInputs().get(0).getValue();
        newJoinOp.getInputs().add(new MutableObject<>(joinOp.getInputs().get(0).getValue()));
        newJoinOp.getInputs().add(new MutableObject<>(fpInputOp));
        fpOp.getInputs().get(0).setValue(newJoinOp);
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
        return true;
    }
}
