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
package org.apache.asterix.graphix.lang.rewrite.print;

import java.io.PrintWriter;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.visitor.SqlppAstPrintVisitor;

public class GraphixASTPrintVisitor extends SqlppAstPrintVisitor implements IGraphixLangVisitor<Void, Integer> {
    public GraphixASTPrintVisitor(PrintWriter out) {
        super(out);
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws CompilationException {
        return (lc instanceof LetGraphClause) ? visit((LetGraphClause) lc, step) : super.visit(lc, step);
    }

    @Override
    public Void visit(LetGraphClause lgc, Integer step) throws CompilationException {
        out.print(skip(step) + "LET ");
        lgc.getVarExpr().accept(this, 0);
        out.println(skip(step + 1) + ":=");
        lgc.getGraphConstructor().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(GraphConstructor gc, Integer step) throws CompilationException {
        out.println(skip(step) + "GRAPH [");
        for (VertexConstructor vertexConstructor : gc.getVertexElements()) {
            vertexConstructor.accept(this, step + 1);
        }
        for (EdgeConstructor edgeConstructor : gc.getEdgeElements()) {
            edgeConstructor.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(EdgeConstructor ee, Integer step) throws CompilationException {
        out.print(skip(step) + "EDGE ");
        out.print("(:" + ee.getSourceLabel() + ")");
        out.print("-[:" + ee.getEdgeLabel() + "]->");
        out.println("(:" + ee.getDestinationLabel() + ")");
        out.println(skip(step) + "AS ");
        ee.getExpression().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(VertexConstructor ve, Integer step) throws CompilationException {
        out.print(skip(step) + "VERTEX ");
        out.println("(:" + ve.getLabel() + ")");
        out.println(skip(step) + "AS ");
        ve.getExpression().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(FromClause fc, Integer step) throws CompilationException {
        return (fc instanceof FromGraphClause) ? this.visit((FromGraphClause) fc, step) : super.visit(fc, step);
    }

    @Override
    public Void visit(FromGraphClause fgc, Integer step) throws CompilationException {
        out.print(skip(step) + "FROM [");
        for (int i = 0; i < fgc.getTerms().size(); i++) {
            AbstractClause fromTerm = fgc.getTerms().get(i);
            if (i > 0) {
                out.println(",");
            }
            fromTerm.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(FromGraphTerm fgt, Integer step) throws CompilationException {
        out.println(skip(step) + "FROM [");
        out.print(skip(step + 1) + "GRAPH ");
        if (fgt.getDataverseName() != null) {
            out.print(fgt.getDataverseName().toString());
            out.print(".");
        }
        out.print(fgt.getGraphName());
        out.println(skip(step) + "]");
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            matchClause.accept(this, step);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            correlateClause.accept(this, step);
        }
        if (fgt.getLowerClause() != null) {
            this.visit(fgt.getLowerClause().getVisitorExtension(), step);
        }
        return null;
    }

    @Override
    public Void visit(MatchClause mc, Integer step) throws CompilationException {
        out.print(skip(step));
        switch (mc.getMatchType()) {
            case LEADING:
            case INNER:
                out.println("MATCH [");
                break;
            case LEFTOUTER:
                out.println("LEFT MATCH [");
                break;
        }
        for (QueryPatternExpr queryPatternExpr : mc) {
            queryPatternExpr.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(QueryPatternExpr qpe, Integer step) throws CompilationException {
        int index = 0;
        for (IPatternConstruct patternConstruct : qpe) {
            if (index > 0) {
                out.println();
            }
            patternConstruct.accept(this, step);
            index++;
        }
        return null;
    }

    @Override
    public Void visit(PathPatternExpr ppe, Integer step) throws CompilationException {
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        ppe.getLeftVertex().accept(this, step);
        if (pathDescriptor.getElementDirection() == ElementDirection.RIGHT_TO_LEFT) {
            out.print(skip(step) + "<-[");
        } else {
            out.print(skip(step) + "-[");
        }
        if (pathDescriptor.getVariableExpr() != null) {
            out.print(pathDescriptor.getVariableExpr().getVar().getValue());
        }
        out.print(":");
        if (pathDescriptor.getLabels().stream().allMatch(ElementLabel::isNegated)) {
            out.print("^");
        }
        out.print("(");
        int index = 0;
        for (ElementLabel label : pathDescriptor.getLabels()) {
            if (index > 0) {
                out.print("|");
            }
            out.print(label);
            index++;
        }
        out.print(")]-");
        ppe.getRightVertex().accept(this, step);
        if (!ppe.getDecompositions().isEmpty()) {
            out.println();
            out.println(skip(step) + "DECOMPOSED INTO [");
            for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
                decomposition.accept(this, step + 1);
            }
            out.println("]");
        }
        return null;
    }

    @Override
    public Void visit(EdgePatternExpr epe, Integer step) throws CompilationException {
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        epe.getLeftVertex().accept(this, step);
        if (edgeDescriptor.getElementDirection() == ElementDirection.RIGHT_TO_LEFT) {
            out.print(skip(step) + "<-[");
        } else {
            out.print(skip(step) + "-[");
        }
        if (edgeDescriptor.getVariableExpr() != null) {
            out.print(edgeDescriptor.getVariableExpr().getVar().getValue());
        }
        out.print(":");
        if (edgeDescriptor.getLabels().stream().allMatch(ElementLabel::isNegated)) {
            out.print("^");
        }
        out.print("(");
        int index = 0;
        for (ElementLabel label : edgeDescriptor.getLabels()) {
            if (index > 0) {
                out.print("|");
            }
            out.print(label);
            index++;
        }
        out.print(")");
        if (edgeDescriptor.getFilterExpr() != null) {
            out.print(" WHERE ");
            edgeDescriptor.getFilterExpr().accept(this, 0);
        }
        out.print(" )");
        out.print("]-");
        epe.getRightVertex().accept(this, step);
        return null;
    }

    @Override
    public Void visit(VertexPatternExpr vpe, Integer step) throws CompilationException {
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        out.print(skip(step) + "(");
        if (vertexDescriptor.getVariableExpr() != null) {
            out.print(vertexDescriptor.getVariableExpr().getVar().getValue());
        }
        out.print(":");
        if (vertexDescriptor.getLabels().stream().allMatch(ElementLabel::isNegated)) {
            out.print("^");
        }
        out.print("(");
        int index = 0;
        for (ElementLabel label : vertexDescriptor.getLabels()) {
            if (index > 0) {
                out.print("|");
            }
            out.print(label);
            index++;
        }
        out.print(")");
        if (vertexDescriptor.getFilterExpr() != null) {
            out.print(" WHERE ");
            vertexDescriptor.getFilterExpr().accept(this, 0);
        }
        out.print(" )");
        return null;
    }

    // The following should not appear in queries (the former, pre-rewrite).
    @Override
    public Void visit(CreateGraphStatement cgs, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(GraphElementDeclaration ged, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(GraphDropStatement gds, Integer step) throws CompilationException {
        return null;
    }
}
