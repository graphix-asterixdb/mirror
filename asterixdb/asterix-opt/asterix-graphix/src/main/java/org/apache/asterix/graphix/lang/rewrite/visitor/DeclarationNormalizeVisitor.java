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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixQueryRewriter;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.commons.lang3.mutable.MutableBoolean;

public class DeclarationNormalizeVisitor extends AbstractGraphixQueryVisitor {
    private final GraphixRewritingContext graphixRewritingContext;
    private final GraphixQueryRewriter graphixQueryRewriter;

    public DeclarationNormalizeVisitor(GraphixRewritingContext graphixRewritingContext,
            GraphixQueryRewriter graphixQueryRewriter) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.graphixQueryRewriter = graphixQueryRewriter;
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        for (GraphElementDeclaration declaration : epe.getDeclarationSet()) {
            graphixQueryRewriter.loadNormalizedGraphElement(graphixRewritingContext, declaration);
        }
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        for (GraphElementDeclaration declaration : vpe.getDeclarationSet()) {
            graphixQueryRewriter.loadNormalizedGraphElement(graphixRewritingContext, declaration);
        }
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
            decomposition.accept(this, arg);

            // We need to make sure that we don't have any nested loops.
            for (GraphElementDeclaration graphElementDeclaration : decomposition.getDeclarationSet()) {
                FindRecursionVisitor findRecursionVisitor = new FindRecursionVisitor();
                Objects.requireNonNullElse(graphElementDeclaration.getNormalizedBody(),
                        graphElementDeclaration.getRawBody()).accept(findRecursionVisitor, null);
                if (findRecursionVisitor.isRecursionFound.isTrue()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                            "Nested path queries are not supported!");
                }
            }
        }
        return super.visit(ppe, arg);
    }

    private static class FindRecursionVisitor extends AbstractGraphixQueryVisitor {
        private final MutableBoolean isRecursionFound = new MutableBoolean(false);

        @Override
        public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
            for (FromGraphTerm fromGraphTerm : fgc.getFromGraphTerms()) {
                AbstractExtensionClause lowerClause = fromGraphTerm.getLowerClause();
                if (lowerClause == null) {
                    continue;
                }
                if (lowerClause instanceof LoopingClause) {
                    isRecursionFound.setTrue();
                }
                IVisitorExtension ve = lowerClause.getVisitorExtension();
                ve.simpleExpressionDispatch(this, arg);
            }
            return super.visit(fgc, arg);
        }

        @Override
        public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
            isRecursionFound.setTrue();
            return super.visit(ppe, arg);
        }
    }
}
