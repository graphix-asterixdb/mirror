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
package org.apache.asterix.graphix.algebra.optimizer.correction;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class FixMarkerSinkOperatorPlacementRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.MARKER_SINK) {
            // Immediately below our marker sink should be an EXCHANGE.
            Mutable<ILogicalOperator> markerSinkInputOpRef = opRef.getValue().getInputs().get(0);
            if (connectSinkAndExchange(opRef, markerSinkInputOpRef)) {
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
                return true;
            }
        }
        return false;
    }

    private boolean connectSinkAndExchange(Mutable<ILogicalOperator> op1Ref, Mutable<ILogicalOperator> op2Ref)
            throws AlgebricksException {
        if (op2Ref.getValue().getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
            return false;
        }

        // We should only be working with 1:1 operators (micro-operators).
        if (op2Ref.getValue().getInputs().size() > 1) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Non 1:1 operator found!");
        }

        // Swap and recurse.
        ILogicalOperator op1 = op1Ref.getValue();
        ILogicalOperator op2 = op2Ref.getValue();
        op2Ref.setValue(op1);
        op1Ref.setValue(op2);
        op1.getInputs().clear();
        op1.getInputs().addAll(op2.getInputs());
        op2.getInputs().clear();
        op2.getInputs().add(op2Ref);
        connectSinkAndExchange(op2Ref, op1.getInputs().get(0));
        return true;
    }
}
