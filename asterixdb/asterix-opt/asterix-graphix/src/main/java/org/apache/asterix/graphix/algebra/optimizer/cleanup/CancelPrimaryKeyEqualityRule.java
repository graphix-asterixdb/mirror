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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;

/**
 * Given a {@link SelectOperator} whose condition contains conjuncts that have equality on the primary keys of a
 * {@link DataSourceScanOperator}, replace the conjuncts with {@link ConstantExpression#TRUE}.
 */
public class CancelPrimaryKeyEqualityRule extends AbstractConditionReplacementRule {
    private final Set<LogicalVariable> dataScanVariables = new HashSet<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        switch (opRef.getValue().getOperatorTag()) {
            case DISTRIBUTE_RESULT:
            case DELEGATE_OPERATOR:
            case SINK:
                // We are at our root. Reset our state.
                dataScanVariables.clear();
                break;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataSourceScanOperator = (DataSourceScanOperator) op;
            dataScanVariables.addAll(dataSourceScanOperator.getVariables());
            return false;
        }
        return super.rewritePost(opRef, context);
    }

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

        // ...and both arguments refer to the same variable...
        if (!firstArgumentVariable.getVariableReference().equals(secondArgumentVariable.getVariableReference())) {
            return false;
        }
        LogicalVariable comparedVariable = firstArgumentVariable.getVariableReference();

        // ...and the compared variable comes from a data-source-scan.
        return dataScanVariables.contains(comparedVariable);
    }

    @Override
    protected void replaceConjunct(Mutable<ILogicalExpression> exprRef) {
        exprRef.setValue(ConstantExpression.TRUE);
    }
}
