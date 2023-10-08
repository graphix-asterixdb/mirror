/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.optimizer.physical;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StateReleasePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * This rule serves to ensure that both schemas into our {@link FixedPointOperator} (anchor and recursive) are exactly
 * the same (similar to {@link org.apache.hyracks.algebricks.rewriter.rules.InsertProjectBeforeUnionRule}).
 * <p>
 * {@link org.apache.hyracks.algebricks.rewriter.rules.PushProjectDownRule} and
 * {@link org.apache.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule} should be run before this.
 */
public class IntroduceProjectBeforeFixedPointRule implements IAlgebraicRewriteRule {
    private final Map<FixedPointOperator, Mutable<ILogicalOperator>> fpHeadOpMap = new LinkedHashMap<>();
    private Mutable<ILogicalOperator> workingRewritePreOp;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        switch (opRef.getValue().getOperatorTag()) {
            case RECURSIVE_HEAD:
                // We have no nested-loops, so we are fine with recording the last seen head op.
                workingRewritePreOp = opRef;
                break;

            case FIXED_POINT:
                FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
                fpHeadOpMap.put(fpOp, workingRewritePreOp);
                break;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();

        // Some sanity checks: we should only be working with a single nested plan with a single root.
        if (fpOp.getNestedPlans().size() != 1 || fpOp.getNestedPlans().get(0).getRoots().size() != 1) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Illegal FIXED-POINT encountered!");
        }
        Mutable<ILogicalOperator> recursiveOutputOpRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
        Mutable<ILogicalOperator> recursiveInputOpRef = fpHeadOpMap.get(fpOp).getValue().getInputs().get(0);
        Mutable<ILogicalOperator> anchorInputOpRef = fpOp.getInputs().get(0);

        // We will use our recursive input (i.e. our recursive head) as our primary "format" / operator schema.
        List<LogicalVariable> recursiveInputVariables = new ArrayList<>();
        VariableUtilities.getLiveVariables(recursiveInputOpRef.getValue(), recursiveInputVariables);
        recursiveInputVariables.sort((v1, v2) -> {
            Collection<LogicalVariable> outputVariables = fpOp.getOutputVariableList();
            // We will move all variables not in-the-loop to the end.
            if (outputVariables.contains(v1) && !outputVariables.contains(v2)) {
                return -1;
            } else if (outputVariables.contains(v2) && !outputVariables.contains(v1)) {
                return 1;
            } else {
                return 0;
            }
        });
        if (hasOperatorsAfterFixedPoint(fpHeadOpMap.get(fpOp).getValue())) {
            insertProjectIntoBranch(recursiveInputOpRef, recursiveInputVariables, () -> {
                byte designationByte = context.getDesignationByteFor(fpHeadOpMap.get(fpOp).getValue());
                IPhysicalOperator projectPOp = new StreamProjectPOperator();
                return new StateReleasePOperator(projectPOp, designationByte);
            }, context);
        }

        // Now, format our anchor input (no state-release is required).
        List<LogicalVariable> anchorInputVariables = new ArrayList<>(recursiveInputVariables);
        anchorInputVariables.replaceAll(v -> fpOp.getInvertedAnchorOutputMap().getOrDefault(v, v));
        insertProjectIntoBranch(anchorInputOpRef, anchorInputVariables, StreamProjectPOperator::new, context);

        // Finally, format our recursive output.
        List<LogicalVariable> recursiveOutputVariables = new ArrayList<>(recursiveInputVariables);
        recursiveOutputVariables.replaceAll(v -> fpOp.getInvertedRecursiveOutputMap().getOrDefault(v, v));
        insertProjectIntoBranch(recursiveOutputOpRef, recursiveOutputVariables, () -> {
            byte designationByte = context.getDesignationByteFor(fpHeadOpMap.get(fpOp).getValue());
            IPhysicalOperator projectPOp = new StreamProjectPOperator();
            return new StateReleasePOperator(projectPOp, designationByte);
        }, context);
        return true;
    }

    private void insertProjectIntoBranch(Mutable<ILogicalOperator> exchangeOpRef, List<LogicalVariable> opSchema,
            Supplier<IPhysicalOperator> projectSupplier, IOptimizationContext context) throws AlgebricksException {
        // Note: this input should be an EXCHANGE.
        if (exchangeOpRef.getValue().getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Input operator should be an EXCHANGE!");
        }
        ILogicalOperator exchangeOp = exchangeOpRef.getValue();

        // Is there an EXCHANGE followed by a PROJECT immediately below our input? If so, we will reuse this.
        Mutable<ILogicalOperator> inputOpRef = exchangeOp.getInputs().get(0);
        if (inputOpRef.getValue().getOperatorTag() == LogicalOperatorTag.PROJECT) {
            ProjectOperator projectOp = (ProjectOperator) inputOpRef.getValue();
            List<LogicalVariable> projectionList = projectOp.getVariables();
            projectionList.clear();
            projectionList.addAll(opSchema);
            projectOp.recomputeSchema();
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(projectOp, context);

        } else {
            // ...otherwise, we need to add our own PROJECT.
            List<LogicalVariable> projectionList = new ArrayList<>();
            ProjectOperator projectOp = new ProjectOperator(projectionList);
            projectionList.addAll(opSchema);
            projectOp.getInputs().add(new MutableObject<>(inputOpRef.getValue()));
            inputOpRef.setValue(projectOp);
            projectOp.setPhysicalOperator(projectSupplier.get());
            projectOp.setExecutionMode(inputOpRef.getValue().getExecutionMode());
            projectOp.recomputeSchema();
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(projectOp, context);
        }
    }

    private boolean hasOperatorsAfterFixedPoint(ILogicalOperator headOp) throws AlgebricksException {
        Deque<ILogicalOperator> operatorQueue = new ArrayDeque<>();
        operatorQueue.add(headOp);
        while (!operatorQueue.isEmpty()) {
            ILogicalOperator workingOp = operatorQueue.removeFirst();
            switch (workingOp.getOperatorTag()) {
                case EXCHANGE:
                case RECURSIVE_HEAD:
                    break;

                case FIXED_POINT:
                    return false;

                default:
                    return true;
            }
            operatorQueue.addLast(workingOp.getInputs().get(0).getValue());
        }
        throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "No FIXED-POINT operator found?");
    }
}
