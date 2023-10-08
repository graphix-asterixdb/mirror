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
import java.util.List;
import java.util.ListIterator;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveAlwaysTrueSelectConditionRule implements IAlgebraicRewriteRule {
    private final ILogicalExpressionReferenceTransform removeTrueConjuncts = exprRef -> {
        boolean hasChanged = false;

        // We are only concerned with conjuncts.
        List<Mutable<ILogicalExpression>> conjunctRefs = new ArrayList<>();
        if (exprRef.getValue().splitIntoConjuncts(conjunctRefs)) {
            for (ListIterator<Mutable<ILogicalExpression>> it = conjunctRefs.listIterator(); it.hasNext();) {
                Mutable<ILogicalExpression> workingOpRef = it.next();
                if (workingOpRef.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression constantExpr = (ConstantExpression) workingOpRef.getValue();
                    if (constantExpr.getValue().isTrue()) {
                        hasChanged = true;
                        it.remove();
                    }
                }
            }
            switch (conjunctRefs.size()) {
                case 0:
                    exprRef.setValue(ConstantExpression.TRUE);
                    break;

                case 1:
                    exprRef.setValue(conjunctRefs.get(0).getValue());
                    break;

                default:
                    FunctionInfo functionInfo = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND);
                    exprRef.setValue(new ScalarFunctionCallExpression(functionInfo, conjunctRefs));
                    break;
            }
        }
        return hasChanged;
    };

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator selectOp = (SelectOperator) opRef.getValue();
        ILogicalOperator inputOp = selectOp.getInputs().get(0).getValue();

        // Remove any TRUE conjuncts.
        boolean hasChanged = selectOp.acceptExpressionTransform(removeTrueConjuncts);
        ILogicalExpression selectCondExpr = selectOp.getCondition().getValue();
        if (selectCondExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constantExpr = (ConstantExpression) selectCondExpr;
            if (constantExpr.getValue().isTrue()) {
                opRef.setValue(inputOp);
                hasChanged = true;
            }
        }

        // Update our type information.
        if (hasChanged) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(inputOp, context);
        }
        return hasChanged;
    }
}
