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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * @see CancelPrimaryKeyEqualityRule
 * @see ReplaceExtraneousEqualityRule
 */
public abstract class AbstractConditionReplacementRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case SELECT:
                SelectOperator selectOperator = (SelectOperator) op;
                return searchAndReplaceConjuncts(selectOperator.getCondition());

            case INNERJOIN:
            case LEFTOUTERJOIN:
                AbstractBinaryJoinOperator joinOperator = (AbstractBinaryJoinOperator) op;
                return searchAndReplaceConjuncts(joinOperator.getCondition());

            default:
                return false;
        }
    }

    protected abstract boolean isReplaceableConjunct(ILogicalExpression expression);

    protected abstract void replaceConjunct(Mutable<ILogicalExpression> exprRef);

    private boolean searchAndReplaceConjuncts(Mutable<ILogicalExpression> condRef) {
        boolean isConditionChanged = true;
        List<Mutable<ILogicalExpression>> conditionConjuncts = new ArrayList<>();
        if (splitIntoConjuncts(condRef.getValue(), conditionConjuncts)) {
            for (Mutable<ILogicalExpression> conditionConjunctRef : conditionConjuncts) {
                ILogicalExpression conditionConjunct = conditionConjunctRef.getValue();
                boolean isConjunctReplaceable = isReplaceableConjunct(conditionConjunct);
                if (isConjunctReplaceable) {
                    replaceConjunct(conditionConjunctRef);
                }
                isConditionChanged &= isConjunctReplaceable;
            }

        } else if (condRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            boolean isConjunctReplaceable = isReplaceableConjunct(condRef.getValue());
            if (isConjunctReplaceable) {
                replaceConjunct(condRef);
            }
            isConditionChanged = isConjunctReplaceable;

        } else {
            isConditionChanged = false;
        }
        return isConditionChanged;
    }

    private boolean splitIntoConjuncts(ILogicalExpression expression, List<Mutable<ILogicalExpression>> conjuncts) {
        List<Mutable<ILogicalExpression>> exprConjuncts = new ArrayList<>();
        if (expression.splitIntoConjuncts(exprConjuncts)) {
            for (Mutable<ILogicalExpression> conjunct : exprConjuncts) {
                List<Mutable<ILogicalExpression>> innerExprConjuncts = new ArrayList<>();
                if (splitIntoConjuncts(conjunct.getValue(), innerExprConjuncts)) {
                    conjuncts.addAll(innerExprConjuncts);
                } else {
                    conjuncts.add(conjunct);
                }
            }
            return true;
        }
        return false;
    }
}
