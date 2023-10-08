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
package org.apache.asterix.graphix.algebra.optimizer.physical;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.graphix.algebra.optimizer.navigation.PromoteSelectInLoopToJoinRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StateReleasePOperator;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;

public class IsolateHyracksOperatorsWithLoopRule extends IsolateHyracksOperatorsRule {
    private final Map<ILogicalOperator, Byte> operatorByteMap = new LinkedHashMap<>();

    public IsolateHyracksOperatorsWithLoopRule(PhysicalOperatorTag[] operatorsBelowWhichJobGenIsDisabled) {
        super(operatorsBelowWhichJobGenIsDisabled);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.RECURSIVE_HEAD) {
            byte designationByte = context.getDesignationByteFor(opRef.getValue());
            Set<ILogicalOperator> operatorsInLoop = new LinkedHashSet<>();
            gathersOpsInLoop(opRef, operatorsInLoop);
            operatorsInLoop.forEach(op -> operatorByteMap.put(op, designationByte));
        }
        return false;
    }

    private void gathersOpsInLoop(Mutable<ILogicalOperator> opRef, Set<ILogicalOperator> operatorsInLoop) {
        ILogicalOperator workingOp = opRef.getValue();
        switch (workingOp.getOperatorTag()) {
            case RECURSIVE_TAIL:
                // Base case: we have reached the end of the loop.
                operatorsInLoop.add(workingOp);
                return;

            case FIXED_POINT:
                // Special case: we have reached our fixed-point and need to go through our nested plan.
                FixedPointOperator fpOp = (FixedPointOperator) workingOp;
                for (ILogicalPlan nestedPlan : fpOp.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                        gathersOpsInLoop(rootRef, operatorsInLoop);
                    }
                }
                operatorsInLoop.add(workingOp);
                return;

            case INNERJOIN:
                // Special case: we have a PBJ. We'll avoid marking the non-recursive flow.
                operatorsInLoop.add(workingOp);
                InnerJoinOperator joinOp = (InnerJoinOperator) workingOp;
                if (joinOp.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.PERSISTENT_BUILD_JOIN) {
                    int transientJoinIndex = PromoteSelectInLoopToJoinRule.JOIN_TRANSIENT_INPUT_INDEX;
                    gathersOpsInLoop(joinOp.getInputs().get(transientJoinIndex), operatorsInLoop);
                }
                return;

            default:
                // Recurse until we reach our fixed-point operator.
                operatorsInLoop.add(workingOp);
                for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                    gathersOpsInLoop(inputRef, operatorsInLoop);
                }
                break;
        }
    }

    @Override
    protected PhysicalOperatorTag getPhysicalOperatorTag(AbstractLogicalOperator op) {
        IPhysicalOperator pOp = op.getPhysicalOperator();
        if (pOp.getOperatorTag() == PhysicalOperatorTag.STATE_RELEASE) {
            return ((StateReleasePOperator) pOp).getPhysicalDelegateOp().getOperatorTag();
        }
        return pOp.getOperatorTag();
    }

    @Override
    protected void insertOneToOneExchange(Mutable<ILogicalOperator> inputRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator inputOp = inputRef.getValue();
        ExchangeOperator exchangeOp = new ExchangeOperator();
        if (operatorByteMap.containsKey(inputOp)) {
            Byte designationByte = operatorByteMap.get(inputOp);
            exchangeOp.setPhysicalOperator(new StateReleasePOperator(new OneToOneExchangePOperator(), designationByte));

        } else {
            exchangeOp.setPhysicalOperator(new OneToOneExchangePOperator());
        }

        // Add our exchange op to the rest of the plan.
        exchangeOp.getInputs().add(new MutableObject<>(inputOp));
        exchangeOp.setExecutionMode(inputOp.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOp);
        exchangeOp.recomputeSchema();
        exchangeOp.computeDeliveredPhysicalProperties(context);
        inputRef.setValue(exchangeOp);
    }
}
