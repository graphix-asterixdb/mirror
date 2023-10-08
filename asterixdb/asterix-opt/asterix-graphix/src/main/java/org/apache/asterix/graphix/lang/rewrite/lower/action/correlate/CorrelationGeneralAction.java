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
package org.apache.asterix.graphix.lang.rewrite.lower.action.correlate;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.ISequenceTransformer;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class CorrelationGeneralAction implements IEnvironmentAction {
    private final List<? extends Expression> leftConjuncts;
    private final List<? extends Expression> rightConjuncts;
    private final List<? extends IExpressionAnnotation> joinHints;

    public CorrelationGeneralAction(List<? extends Expression> leftList, List<? extends Expression> rightList,
            List<? extends IExpressionAnnotation> joinHints) {
        this.leftConjuncts = Objects.requireNonNull(leftList);
        this.rightConjuncts = Objects.requireNonNull(rightList);
        this.joinHints = Objects.requireNonNull(joinHints);
    }

    public CorrelationGeneralAction(List<? extends Expression> leftList, List<? extends Expression> rightList) {
        this.leftConjuncts = Objects.requireNonNull(leftList);
        this.rightConjuncts = Objects.requireNonNull(rightList);
        this.joinHints = List.of();
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        loweringEnvironment.acceptTransformer(new ISequenceTransformer() {
            public void propagateHints(Expression expression) {
                if (expression.getKind() != Expression.Kind.OP_EXPRESSION) {
                    // We should only continue if we have an operator expression we can attach JOIN hints to.
                    return;
                }
                OperatorExpr operatorExpr = (OperatorExpr) expression;

                // Iterate over our arguments.
                Iterator<Expression> exprIterator = operatorExpr.getExprList().iterator();
                for (OperatorType operatorType : operatorExpr.getOpList()) {
                    Expression leftExpr = exprIterator.next();
                    Expression rightExpr = exprIterator.next();
                    if (operatorType.equals(OperatorType.EQ)) {
                        for (IExpressionAnnotation joinHint : joinHints) {
                            operatorExpr.addHint(joinHint);
                        }
                    } else {
                        propagateHints(leftExpr);
                        propagateHints(rightExpr);
                    }
                }
            }

            @Override
            public void accept(ClauseSequence clauseSequence) {
                Expression joinCondExpr = LowerRewritingUtil.buildSingleElementJoin(leftConjuncts, rightConjuncts);
                if (!joinHints.isEmpty() && joinCondExpr.getKind() == Expression.Kind.OP_EXPRESSION) {
                    propagateHints(joinCondExpr);
                }
                clauseSequence.addNonRepresentativeClause(new WhereClause(joinCondExpr));
            }
        });
    }
}
