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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.EvaluationPreferIndexNLOption;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.CorrelationJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.LeftLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.NavigationJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.RightLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class PlaceEvaluationHintsVisitor extends AbstractGraphixQueryVisitor {
    private final EvaluationPreferIndexNLOption preferIndexNLOption;

    public PlaceEvaluationHintsVisitor(GraphixRewritingContext graphixRewritingContext) throws CompilationException {
        preferIndexNLOption = (EvaluationPreferIndexNLOption) graphixRewritingContext
                .getSetting(EvaluationPreferIndexNLOption.OPTION_KEY_NAME);
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        if (preferIndexNLOption == EvaluationPreferIndexNLOption.FALSE) {
            // We have no hints to add. Exit early here.
            return null;
        }
        return super.visit(fgt, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        if (vpe.getParentVertexExpr() != null && (!vpe.hasHints() || vpe.getHints().isEmpty())) {
            List<IExpressionAnnotation> hintList = new ArrayList<>();
            hintList.add(new CorrelationJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            vpe.addHints(hintList);
        }
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        if (!epe.hasHints() || epe.getHints().isEmpty()) {
            List<IExpressionAnnotation> hintList = new ArrayList<>();
            hintList.add(new LeftLinkJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            hintList.add(new RightLinkJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            epe.addHints(hintList);
        }
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        if (!ppe.hasHints() || ppe.getHints().isEmpty()) {
            List<IExpressionAnnotation> hintList = new ArrayList<>();
            hintList.add(new LeftLinkJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            hintList.add(new RightLinkJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            hintList.add(new NavigationJoinAnnotation(IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX));
            ppe.addHints(hintList);
        }
        return super.visit(ppe, arg);
    }
}
