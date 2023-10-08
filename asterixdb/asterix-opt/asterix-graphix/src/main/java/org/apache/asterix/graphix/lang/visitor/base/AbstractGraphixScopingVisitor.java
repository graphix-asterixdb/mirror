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
package org.apache.asterix.graphix.lang.visitor.base;

import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.IMappingConstruct;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.scope.ScopedCollection;
import org.apache.asterix.graphix.lang.rewrite.util.ExpressionScopeUtil;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.CheckSql92AggregateVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class AbstractGraphixScopingVisitor extends AbstractSqlppExpressionScopingVisitor
        implements IGraphixLangVisitor<Expression, ILangExpression> {
    private final CheckSql92AggregateVisitor checkSql92AggregateVisitor = new CheckSql92AggregateVisitor();
    private final ScopedCollection<Pair<GraphConstructor, String>> graphConstructors = new ScopedCollection<>();

    protected final GraphixRewritingContext graphixRewritingContext;

    public AbstractGraphixScopingVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
        this.graphixRewritingContext = graphixRewritingContext;
    }

    protected GraphConstructor getGraphConstructor(GraphIdentifier graphIdentifier) {
        return graphConstructors.getElementsInScope(scopeChecker.getCurrentScope()).stream()
                .filter(p -> p.getSecond().equals(graphIdentifier.getGraphName())).map(Pair::getFirst).findFirst()
                .orElse(null);
    }

    @Override
    public Expression visit(LetClause lc, ILangExpression arg) throws CompilationException {
        return (lc instanceof LetGraphClause) ? visit((LetGraphClause) lc, arg) : super.visit(lc, arg);
    }

    @Override
    public Expression visit(LetGraphClause lgc, ILangExpression arg) throws CompilationException {
        // Visit our graph constructor.
        lgc.getGraphConstructor().accept(this, lgc);

        // Now consider our graph name.
        Scope currentScope = scopeChecker.getCurrentScope();
        VarIdentifier nameIdentifier = lgc.getVarExpr().getVar();
        String graphName = SqlppVariableUtil.toUserDefinedName(nameIdentifier.getValue());
        if (scopeChecker.lookupSymbol(nameIdentifier.getValue()) != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, lgc.getSourceLocation(),
                    "Duplicate graph definitions: " + graphName);
        }
        graphConstructors.addElement(new Pair<>(lgc.getGraphConstructor(), graphName), nameIdentifier, currentScope);
        currentScope.addNewVarSymbolToScope(nameIdentifier);
        return null;
    }

    @Override
    public Expression visit(FromClause fc, ILangExpression arg) throws CompilationException {
        return (fc instanceof FromGraphClause) ? visit((FromGraphClause) fc, arg) : super.visit(fc, arg);
    }

    @Override
    public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
        Scope scopeForFromClause = scopeChecker.extendCurrentScope();
        for (AbstractClause fromTerm : fgc.getTerms()) {
            fromTerm.accept(this, fgc);
            Scope scopeForFromTerm = scopeChecker.removeCurrentScope();
            mergeScopes(scopeForFromClause, scopeForFromTerm, fromTerm.getSourceLocation());
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        // We are now working with a new scope.
        scopeChecker.createNewScope();
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            matchClause.accept(this, arg);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(MatchClause mc, ILangExpression arg) throws CompilationException {
        for (QueryPatternExpr queryPatternExpr : mc) {
            queryPatternExpr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(QueryPatternExpr qpe, ILangExpression arg) throws CompilationException {
        // Visit all of our vertices first, then our edges and paths.
        for (IPatternConstruct patternConstruct : qpe) {
            if (patternConstruct.getPatternType() == IPatternConstruct.PatternType.VERTEX_PATTERN) {
                patternConstruct.accept(this, arg);
            }
        }
        for (IPatternConstruct patternConstruct : qpe) {
            if (patternConstruct.getPatternType() != IPatternConstruct.PatternType.VERTEX_PATTERN) {
                patternConstruct.accept(this, arg);
            }
        }
        return qpe;
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        // We do not visit any vertices here.
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        if (pathDescriptor.getVariableExpr() != null) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(pathDescriptor.getVariableExpr().getVar());
        }
        return ppe;
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        // We do not visit any vertices here.
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() != null) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(edgeDescriptor.getVariableExpr().getVar());
        }
        if (edgeDescriptor.getFilterExpr() != null) {
            edgeDescriptor.getFilterExpr().accept(this, arg);
        }
        return epe;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        if (vertexDescriptor.getVariableExpr() != null) {
            // We should have already verified whether a vertex variable is valid.
            VarIdentifier vertexId = vertexDescriptor.getVariableExpr().getVar();
            if (scopeChecker.getCurrentScope().findLocalSymbol(vertexId.getValue()) == null) {
                scopeChecker.getCurrentScope().addNewVarSymbolToScope(vertexId);
            }
        }
        if (vertexDescriptor.getFilterExpr() != null) {
            vertexDescriptor.getFilterExpr().accept(this, arg);
        }
        return vpe;
    }

    // We will defer the scope of SQL-92 aggregates to our AggregationSugarVisitor.
    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        return (checkSql92AggregateVisitor.visit(callExpr, arg)) ? callExpr : super.visit(callExpr, arg);
    }

    // We aren't going to inline our column aliases (yet), so add our select variables to our scope.
    @Override
    public Expression visit(SelectClause sc, ILangExpression arg) throws CompilationException {
        super.visit(sc, arg);
        if (sc.selectRegular()) {
            SelectRegular selectRegular = sc.getSelectRegular();
            for (Projection projection : selectRegular.getProjections()) {
                String variableName = SqlppVariableUtil.toInternalVariableName(projection.getName());
                scopeChecker.getCurrentScope().addSymbolToScope(new Identifier(variableName), Set.of());
            }
        }
        return null;
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
        if (!ExpressionScopeUtil.isForGraphixScopeChecking(fa, arg)) {
            // This is a field-accessor from a FROM-TERM. Ignore this, and let our SQL++ rewrites handle this.
            return fa;
        }
        return super.visit(fa, arg);
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        for (IMappingConstruct mappingConstructor : gc) {
            mappingConstructor.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(EdgeConstructor ee, ILangExpression arg) throws CompilationException {
        scopeChecker.createNewScope();
        ee.getExpression().accept(this, arg);
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Expression visit(VertexConstructor ve, ILangExpression arg) throws CompilationException {
        scopeChecker.createNewScope();
        ve.getExpression().accept(this, arg);
        scopeChecker.removeCurrentScope();
        return null;
    }

    // The following should not appear in queries.
    @Override
    public Expression visit(CreateGraphStatement cgs, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphElementDeclaration ged, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphDropStatement gds, ILangExpression arg) throws CompilationException {
        return null;
    }
}
