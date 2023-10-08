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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Given an {@link AbstractFunctionCallExpression} of ID {@link BuiltinFunctions#SWITCH_CASE} where the condition is
 * always equal to one of the conditions, remove the {@link AbstractFunctionCallExpression} altogether. This is used to
 * satisfy a very narrow use-case, where we generate such a pattern to handle undirected-edges in Graphix.
 */
public class RemoveConstantSwitchCaseRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op;

        // We are looking for an ASSIGN...
        boolean hasChanged = false;
        for (Mutable<ILogicalExpression> exprRef : assignOp.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();

            // ...whose expression includes a FUNCTION-CALL...
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            // ...of SWITCH-CASE...
            if (!Objects.equals(funcExpr.getFunctionIdentifier(), BuiltinFunctions.SWITCH_CASE)) {
                continue;
            }
            List<Mutable<ILogicalExpression>> funcArgs = funcExpr.getArguments();
            Iterator<Mutable<ILogicalExpression>> funcArgsIterator = funcArgs.iterator();

            // ...where the condition is a constant integer expression...
            Integer conditionInteger = getConstantValue(funcArgsIterator.next().getValue());
            if (conditionInteger == null) {
                continue;
            }

            // ...and where the argument is equal to the condition.
            while (funcArgsIterator.hasNext()) {
                Mutable<ILogicalExpression> caseExpr = funcArgsIterator.next();
                Mutable<ILogicalExpression> resultExpr = funcArgsIterator.next();
                Integer caseConditionArg = getConstantValue(caseExpr.getValue());
                if (Objects.equals(caseConditionArg, conditionInteger)) {
                    // We have met all the conditions. Remove this SWITCH-CASE.
                    exprRef.setValue(resultExpr.getValue());
                    hasChanged = true;
                }
            }
        }
        return hasChanged;
    }

    private Integer getConstantValue(ILogicalExpression conditionExpr) {
        if (conditionExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        ConstantExpression conditionConstantExpr = (ConstantExpression) conditionExpr;
        if (!(conditionConstantExpr.getValue() instanceof AsterixConstantValue)) {
            return null;
        }
        AsterixConstantValue conditionConstantValue = (AsterixConstantValue) conditionConstantExpr.getValue();
        if (conditionConstantValue.getObject().getType() != BuiltinType.AINT32) {
            return null;
        }
        AInt32 conditionConstantInteger = (AInt32) conditionConstantValue.getObject();
        return conditionConstantInteger.getIntegerValue();
    }
}
