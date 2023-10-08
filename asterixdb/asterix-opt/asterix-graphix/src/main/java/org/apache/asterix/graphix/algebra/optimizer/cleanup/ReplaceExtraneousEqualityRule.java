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

import java.util.Objects;

import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

/**
 * Replace equality between two of the same variable (say, {@code v == v}), with an explicit call to
 * {@link BuiltinFunctions#NOT} ({@link BuiltinFunctions#IS_UNKNOWN} ({@code v})). This rule should be run after
 * {@link CancelPrimaryKeyEqualityRule}, as primary key values are implicitly not unknown.
 */
public class ReplaceExtraneousEqualityRule extends AbstractConditionReplacementRule {
    @Override
    protected boolean isReplaceableConjunct(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) expression;

        // We should be working with an equality...
        if (!Objects.equals(functionCallExpression.getFunctionIdentifier(), BuiltinFunctions.EQ)) {
            return false;
        }

        // ...where the first argument is a variable reference...
        ILogicalExpression firstArgument = functionCallExpression.getArguments().get(0).getValue();
        if (firstArgument.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression firstArgumentVariable = (VariableReferenceExpression) firstArgument;

        // ...and the second argument is also a variable reference...
        ILogicalExpression secondArgument = functionCallExpression.getArguments().get(1).getValue();
        if (secondArgument.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression secondArgumentVariable = (VariableReferenceExpression) secondArgument;

        // ...and both arguments refer to the same variable.
        return firstArgumentVariable.getVariableReference().equals(secondArgumentVariable.getVariableReference());
    }

    @Override
    protected void replaceConjunct(Mutable<ILogicalExpression> exprRef) {
        // Note: the following casts are valid, as the check above should have passed.
        AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) exprRef.getValue();
        ILogicalExpression firstArgument = functionCallExpression.getArguments().get(0).getValue();
        VariableReferenceExpression firstArgumentVariable = (VariableReferenceExpression) firstArgument;
        AbstractLogicalExpression variableClone = firstArgumentVariable.cloneExpression();

        // Replace our conjunct EQ(v,v) with NOT(IS_UNKNOWN(v)).
        BuiltinFunctionInfo isUnknownInfo = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.IS_UNKNOWN);
        BuiltinFunctionInfo notInfo = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NOT);
        ILogicalExpression isUnknownFunctionCallExpr =
                new ScalarFunctionCallExpression(isUnknownInfo, new MutableObject<>(variableClone));
        exprRef.setValue(new ScalarFunctionCallExpression(notInfo, new MutableObject<>(isUnknownFunctionCallExpr)));
    }
}
