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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class IntroduceExchangeInFixedPointRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT
                && !context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
        if (fpOp.getNestedPlans().size() != 1 || fpOp.getNestedPlans().get(0).getRoots().size() != 1) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                    "Cannot compile fixed-point plans with more than one root!");
        }

        // Does this operator have an exchange operator at the root?
        Mutable<ILogicalOperator> planInLoopRootRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
        if (planInLoopRootRef.getValue().getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            ExchangeOperator exchangeOp = new ExchangeOperator();
            exchangeOp.setSourceLocation(fpOp.getSourceLocation());
            exchangeOp.setPhysicalOperator(new OneToOneExchangePOperator());
            exchangeOp.getInputs().add(new MutableObject<>(planInLoopRootRef.getValue()));
            exchangeOp.setSchema(planInLoopRootRef.getValue().getSchema());
            planInLoopRootRef.setValue(exchangeOp);
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(planInLoopRootRef.getValue(), context);
            context.addToDontApplySet(this, fpOp);
            return true;
        }
        return false;
    }
}
