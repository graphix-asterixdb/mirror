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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixScopingVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

/**
 * Ensure that any subtree whose immediate parent node includes a {@link FromGraphTerm} follow Graphix-specific
 * variable resolution rules (i.e. do not rely on context variables).
 * <ul>
 *  <li>{@link QueryPatternExpr} may introduce a variable. Uniqueness is enforced here.</li>
 *  <li>{@link EdgePatternExpr} may introduce a variable. Uniqueness is enforced w/ {@link PreRewriteCheckVisitor}.</li>
 *  <li>{@link PathPatternExpr} may introduce a variable. Uniqueness is enforced w/ {@link PreRewriteCheckVisitor}.</li>
 *  <li>{@link VertexPatternExpr} may introduce a variable. Uniqueness is not required.</li>
 *  <li>{@link org.apache.asterix.lang.sqlpp.clause.JoinClause} may introduce a variable (handled in parent).</li>
 *  <li>{@link org.apache.asterix.lang.sqlpp.clause.NestClause} may introduce a variable (handled in parent).</li>
 *  <li>{@link org.apache.asterix.lang.sqlpp.clause.UnnestClause} may introduce a variable (handled in parent).</li>
 *  <li>{@link org.apache.asterix.lang.common.clause.GroupbyClause} may introduce a variable (handled in parent).</li>
 *  <li>{@link org.apache.asterix.lang.common.clause.LetClause} may introduce a variable (handled in parent).</li>
 * </ul>
 * We assume that the following rewrites have already been invoked:
 * <ol>
 *   <li>{@link PreRewriteCheckVisitor}</li>
 *   <li>{@link FillStartingUnknownsVisitor}</li>
 * </ol>
 */
public class ExpressionScopingVisitor extends AbstractGraphixScopingVisitor {
    private final Deque<Mutable<Boolean>> fromGraphVisitStack = new ArrayDeque<>();

    public ExpressionScopingVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);

        // We start with an element of false in our stack.
        fromGraphVisitStack.addLast(new MutableObject<>(false));
    }

    @Override
    public Expression visit(SelectExpression se, ILangExpression arg) throws CompilationException {
        fromGraphVisitStack.addLast(new MutableObject<>(false));
        super.visit(se, arg);
        fromGraphVisitStack.removeLast();
        return se;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        fromGraphVisitStack.getLast().setValue(true);
        return super.visit(fgt, arg);
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        // Don't raise any scoping errors with our constructor body here (we'll leave this to the SQL++ rewrites).
        return null;
    }

    @Override
    public Expression visit(VariableExpr v, ILangExpression arg) throws CompilationException {
        boolean hasVisitedGraphixNode = !fromGraphVisitStack.isEmpty() && fromGraphVisitStack.getLast().getValue();
        String varSymbol = v.getVar().getValue();

        // We will only throw an unresolved error if we first encounter a Graphix AST node.
        if (hasVisitedGraphixNode && scopeChecker.getCurrentScope().findSymbol(varSymbol) == null) {
            throw new CompilationException(ErrorCode.UNDEFINED_IDENTIFIER, v.getSourceLocation(),
                    SqlppVariableUtil.toUserDefinedVariableName(varSymbol).getValue());
        }
        return v;
    }
}
