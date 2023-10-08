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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
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
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;

public abstract class AbstractGraphixQueryVisitor extends AbstractSqlppSimpleExpressionVisitor
        implements IGraphixLangVisitor<Expression, ILangExpression> {
    @Override
    public Expression visit(LetClause lc, ILangExpression arg) throws CompilationException {
        return (lc instanceof LetGraphClause) ? visit((LetGraphClause) lc, arg) : super.visit(lc, arg);
    }

    @Override
    public Expression visit(LetGraphClause lgc, ILangExpression arg) throws CompilationException {
        lgc.getGraphConstructor().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        // Visit all vertices before visiting any edges.
        for (VertexConstructor vertexConstructor : gc.getVertexElements()) {
            vertexConstructor.accept(this, arg);
        }
        for (EdgeConstructor edgeConstructor : gc.getEdgeElements()) {
            edgeConstructor.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(VertexConstructor ve, ILangExpression arg) throws CompilationException {
        return this.visit(ve.getExpression(), arg);
    }

    @Override
    public Expression visit(EdgeConstructor ee, ILangExpression arg) throws CompilationException {
        return this.visit(ee.getExpression(), arg);
    }

    @Override
    public Expression visit(FromClause fc, ILangExpression arg) throws CompilationException {
        return (fc instanceof FromGraphClause) ? visit((FromGraphClause) fc, arg) : super.visit(fc, arg);
    }

    @Override
    public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
        for (AbstractClause fromTerm : fgc.getTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        // Visit all of our MATCH clauses, then our correlate clauses.
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
        // Visit our patterns in order of adjacency.
        for (IPatternConstruct patternConstruct : qpe) {
            patternConstruct.accept(this, arg);
        }
        return qpe;
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        // We do not visit any terminal or internal elements here.
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        if (pathDescriptor.getVariableExpr() != null) {
            pathDescriptor.getVariableExpr().accept(this, arg);
        }
        return ppe;
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        // We do not visit any vertices here or parent paths here.
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() != null) {
            edgeDescriptor.getVariableExpr().accept(this, arg);
        }
        if (edgeDescriptor.getFilterExpr() != null) {
            edgeDescriptor.getFilterExpr().accept(this, arg);
        }
        return epe;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        // We do not visit any source vertices or parent paths here.
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        if (vertexDescriptor.getVariableExpr() != null) {
            vertexDescriptor.getVariableExpr().accept(this, arg);
        }
        if (vertexDescriptor.getFilterExpr() != null) {
            vertexDescriptor.getFilterExpr().accept(this, arg);
        }
        return vpe;
    }

    @Override
    public Expression visit(GraphElementDeclaration ged, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(CreateGraphStatement cgs, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(GraphDropStatement gds, ILangExpression arg) throws CompilationException {
        return null;
    }
}
