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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractExtractExprRule;

/**
 * Needed only bc. current Hyracks operators require keys to be fields.
 */
public class ExtractDistinctByExpressionsRule extends AbstractExtractExprRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.DISTINCT) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        DistinctOperator d = (DistinctOperator) op1;
        boolean changed = false;
        Mutable<ILogicalOperator> opRef2 = d.getInputs().get(0);
        List<Mutable<ILogicalExpression>> newExprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (Mutable<ILogicalExpression> expr : d.getExpressions()) {
            LogicalExpressionTag tag = expr.getValue().getExpressionTag();
            if (tag == LogicalExpressionTag.VARIABLE || tag == LogicalExpressionTag.CONSTANT) {
                newExprList.add(expr);
                continue;
            }
            LogicalVariable v = extractExprIntoAssignOpRef(expr.getValue(), opRef2, context);
            VariableReferenceExpression newExpr = new VariableReferenceExpression(v);
            newExpr.setSourceLocation(expr.getValue().getSourceLocation());
            newExprList.add(new MutableObject<ILogicalExpression>(newExpr));
            changed = true;
        }
        if (changed) {
            d.getExpressions().clear();
            d.getExpressions().addAll(newExprList);
            context.computeAndSetTypeEnvironmentForOperator(d);
        }
        return changed;
    }

}
