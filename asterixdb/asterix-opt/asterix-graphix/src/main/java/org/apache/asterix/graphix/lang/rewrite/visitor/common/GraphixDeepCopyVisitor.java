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
package org.apache.asterix.graphix.lang.rewrite.visitor.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;

/**
 * An extension of {@link DeepCopyVisitor} to include Graphix AST nodes. Note that while deep copies are made here,
 * {@link VertexPatternExpr} cloned references are still maintained for edges and paths.
 */
public class GraphixDeepCopyVisitor extends DeepCopyVisitor implements IGraphixLangVisitor<ILangExpression, Void> {
    private final Map<VertexPatternExpr, VertexPatternExpr> vertexCloneMapping = new HashMap<>();

    @Override
    public SelectBlock visit(SelectBlock sb, Void arg) throws CompilationException {
        SelectClause clonedSelectClause = this.visit(sb.getSelectClause(), arg);
        FromClause clonedFromClause = null;
        if (sb.hasFromClause() && sb.getFromClause() instanceof FromGraphClause) {
            clonedFromClause = this.visit((FromGraphClause) sb.getFromClause(), arg);

        } else if (sb.hasFromClause()) {
            clonedFromClause = super.visit(sb.getFromClause(), arg);
        }
        GroupbyClause clonedGroupByClause = null;
        if (sb.hasGroupbyClause()) {
            clonedGroupByClause = this.visit(sb.getGroupbyClause(), arg);
        }
        List<AbstractClause> clonedLetWhereClauses = new ArrayList<>();
        List<AbstractClause> clonedLetHavingClauses = new ArrayList<>();
        for (AbstractClause letWhereClause : sb.getLetWhereList()) {
            clonedLetWhereClauses.add((AbstractClause) letWhereClause.accept(this, arg));
        }
        for (AbstractClause letHavingClause : sb.getLetHavingListAfterGroupby()) {
            clonedLetHavingClauses.add((AbstractClause) letHavingClause.accept(this, arg));
        }
        SelectBlock clonedSelectBlock = new SelectBlock(clonedSelectClause, clonedFromClause, clonedLetWhereClauses,
                clonedGroupByClause, clonedLetHavingClauses);
        clonedSelectBlock.setSourceLocation(sb.getSourceLocation());
        return clonedSelectBlock;
    }

    @Override
    public LetClause visit(LetClause lc, Void arg) throws CompilationException {
        return (lc instanceof LetGraphClause) ? visit((LetGraphClause) lc, arg) : super.visit(lc, arg);
    }

    @Override
    public LetGraphClause visit(LetGraphClause lgc, Void arg) throws CompilationException {
        VariableExpr clonedVariableExpr = this.visit(lgc.getVarExpr(), arg);
        GraphConstructor clonedGraphConstructor = this.visit(lgc.getGraphConstructor(), arg);
        LetGraphClause clonedLetGraphClause = new LetGraphClause(clonedVariableExpr, clonedGraphConstructor);
        clonedLetGraphClause.setSourceLocation(lgc.getSourceLocation());
        return clonedLetGraphClause;
    }

    @Override
    public FromClause visit(FromClause fc, Void arg) throws CompilationException {
        return (fc instanceof FromGraphClause) ? visit((FromGraphClause) fc, arg) : super.visit(fc, arg);
    }

    @Override
    public FromGraphClause visit(FromGraphClause fgc, Void arg) throws CompilationException {
        List<AbstractClause> clonedFromTerms = new ArrayList<>();
        for (AbstractClause fromTerm : fgc.getTerms()) {
            clonedFromTerms.add((AbstractClause) fromTerm.accept(this, null));
        }
        FromGraphClause clonedFromGraphClause = new FromGraphClause(clonedFromTerms);
        clonedFromGraphClause.setSourceLocation(fgc.getSourceLocation());
        return clonedFromGraphClause;
    }

    @Override
    public FromGraphTerm visit(FromGraphTerm fgt, Void arg) throws CompilationException {
        List<AbstractBinaryCorrelateClause> clonedCorrelateClauses = new ArrayList<>();
        List<MatchClause> clonedMatchClauses = new ArrayList<>();
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            clonedCorrelateClauses.add((AbstractBinaryCorrelateClause) correlateClause.accept(this, arg));
        }
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            clonedMatchClauses.add(this.visit(matchClause, arg));
        }
        FromGraphTerm clonedFromGraphTerm = new FromGraphTerm(fgt.getDataverseName(), fgt.getGraphName(),
                clonedMatchClauses, clonedCorrelateClauses);
        clonedFromGraphTerm.setSourceLocation(fgt.getSourceLocation());
        return clonedFromGraphTerm;
    }

    @Override
    public MatchClause visit(MatchClause mc, Void arg) throws CompilationException {
        List<QueryPatternExpr> clonedQueryPatternExprs = new ArrayList<>();
        for (QueryPatternExpr queryPatternExpr : mc) {
            clonedQueryPatternExprs.add(this.visit(queryPatternExpr, arg));
        }
        MatchClause clonedMatchClause = new MatchClause(clonedQueryPatternExprs, mc.getMatchType());
        clonedMatchClause.setSourceLocation(mc.getSourceLocation());
        return clonedMatchClause;
    }

    @Override
    public QueryPatternExpr visit(QueryPatternExpr qpe, Void arg) throws CompilationException {
        // First, visit and deep-copy all of our vertices.
        VertexPatternExpr leadingClonedVertex = null;
        for (IPatternConstruct patternConstruct : qpe) {
            if (patternConstruct.getPatternType() == IPatternConstruct.PatternType.VERTEX_PATTERN) {
                VertexPatternExpr clonedVertex = this.visit((VertexPatternExpr) patternConstruct, arg);
                if (leadingClonedVertex == null) {
                    leadingClonedVertex = clonedVertex;
                }
            }
        }

        // Next, visit our edges and paths. These will use the vertices from before.
        QueryPatternExpr.Builder queryPatternBuilder = new QueryPatternExpr.Builder(leadingClonedVertex);
        for (IPatternConstruct patternConstruct : qpe) {
            switch (patternConstruct.getPatternType()) {
                case EDGE_PATTERN:
                    queryPatternBuilder.addEdge(this.visit((EdgePatternExpr) patternConstruct, arg));
                    break;
                case PATH_PATTERN:
                    queryPatternBuilder.addPath(this.visit((PathPatternExpr) patternConstruct, arg));
                    break;
            }
        }

        // Finally, build our query pattern.
        QueryPatternExpr clonedQueryPatternExpr = queryPatternBuilder.build();
        clonedQueryPatternExpr.setSourceLocation(qpe.getSourceLocation());
        return clonedQueryPatternExpr;
    }

    @Override
    public VertexPatternExpr visit(VertexPatternExpr vpe, Void arg) throws CompilationException {
        // Build a cloned vertex descriptor.
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        VariableExpr clonedVariableExpr = null;
        if (vertexDescriptor.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(vertexDescriptor.getVariableExpr(), arg);
        }
        Expression clonedFilterExpr = null;
        if (vertexDescriptor.getFilterExpr() != null) {
            clonedFilterExpr = (Expression) vertexDescriptor.getFilterExpr().accept(this, arg);
        }
        Set<ElementLabel> clonedLabels = new HashSet<>(vertexDescriptor.getLabels());
        VertexDescriptor clonedVertexDescriptor =
                new VertexDescriptor(clonedVariableExpr, clonedLabels, clonedFilterExpr);
        if (vertexDescriptor.isVariableGenerated()) {
            // Note: we set our variable expression here to raise the isVariableGenerated flag.
            clonedVertexDescriptor.setVariableExpr(clonedVariableExpr);
        }

        // Build our cloned vertex pattern.
        VertexPatternExpr clonedVertexPatternExpr = new VertexPatternExpr(clonedVertexDescriptor);
        clonedVertexPatternExpr.setSourceLocation(vpe.getSourceLocation());
        for (GraphElementDeclaration declaration : vpe.getDeclarationSet()) {
            clonedVertexPatternExpr.addVertexDeclaration(declaration);
        }
        for (Vertex vertexInfo : vpe.getVertexInfoSet()) {
            clonedVertexPatternExpr.addVertexInfo(vertexInfo);
        }
        if (vpe.hasHints()) {
            clonedVertexPatternExpr.addHints(vpe.getHints());
        }
        if (vpe.arePropertiesUsed()) {
            clonedVertexPatternExpr.markPropertiesAsUsed();
        }
        if (vpe.getParentVertexExpr() != null) {
            clonedVertexPatternExpr.setParentVertexExpr(vertexCloneMapping.get(vpe.getParentVertexExpr()));
        }
        vertexCloneMapping.put(vpe, clonedVertexPatternExpr);
        return clonedVertexPatternExpr;
    }

    @Override
    public EdgePatternExpr visit(EdgePatternExpr epe, Void arg) throws CompilationException {
        // Build a cloned edge descriptor.
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        VariableExpr clonedVariableExpr = null;
        if (edgeDescriptor.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(edgeDescriptor.getVariableExpr(), arg);
        }
        Expression clonedFilterExpr = null;
        if (edgeDescriptor.getFilterExpr() != null) {
            clonedFilterExpr = (Expression) edgeDescriptor.getFilterExpr().accept(this, arg);
        }
        Set<ElementLabel> clonedLabels = new HashSet<>(edgeDescriptor.getLabels());
        EdgeDescriptor clonedEdgeDescriptor = new EdgeDescriptor(edgeDescriptor.getElementDirection(),
                clonedVariableExpr, clonedLabels, clonedFilterExpr);
        if (edgeDescriptor.isVariableGenerated()) {
            // Note: we set our variable expression here to raise the isVariableGenerated flag.
            clonedEdgeDescriptor.setVariableExpr(clonedVariableExpr);
        }

        // Build our cloned edge pattern.
        VertexPatternExpr leftClonedVertex = vertexCloneMapping.get(epe.getLeftVertex());
        VertexPatternExpr rightClonedVertex = vertexCloneMapping.get(epe.getRightVertex());
        EdgePatternExpr clonedEdgePattern =
                new EdgePatternExpr(leftClonedVertex, rightClonedVertex, clonedEdgeDescriptor);
        if (epe.hasHints()) {
            clonedEdgePattern.addHints(epe.getHints());
        }
        for (GraphElementDeclaration declaration : epe.getDeclarationSet()) {
            clonedEdgePattern.addEdgeDeclaration(declaration);
        }
        for (Edge edgeInfo : epe.getEdgeInfoSet()) {
            clonedEdgePattern.addEdgeInfo(edgeInfo);
        }
        clonedEdgePattern.setSourceLocation(epe.getSourceLocation());
        return clonedEdgePattern;
    }

    @Override
    public PathPatternExpr visit(PathPatternExpr ppe, Void arg) throws CompilationException {
        // Build a cloned path descriptor.
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        VariableExpr clonedVariableExpr = null;
        if (pathDescriptor.getVariableExpr() != null) {
            clonedVariableExpr = this.visit(pathDescriptor.getVariableExpr(), arg);
        }
        Set<ElementLabel> clonedLabels = new HashSet<>(pathDescriptor.getLabels());
        PathDescriptor clonedPathDescriptor = new PathDescriptor(pathDescriptor.getElementDirection(),
                clonedVariableExpr, clonedLabels, pathDescriptor.getMinimumHops(), pathDescriptor.getMaximumHops());
        if (pathDescriptor.isVariableGenerated()) {
            // Note: we set our variable expression here to raise the isVariableGenerated flag.
            clonedPathDescriptor.setVariableExpr(clonedVariableExpr);
        }

        // Build our cloned path pattern.
        VertexPatternExpr leftClonedVertex = vertexCloneMapping.get(ppe.getLeftVertex());
        VertexPatternExpr rightClonedVertex = vertexCloneMapping.get(ppe.getRightVertex());
        PathPatternExpr clonedPathPatternExpr =
                new PathPatternExpr(leftClonedVertex, rightClonedVertex, clonedPathDescriptor);
        clonedPathPatternExpr.setSourceLocation(ppe.getSourceLocation());
        if (ppe.hasHints()) {
            clonedPathPatternExpr.addHints(ppe.getHints());
        }
        if (ppe.arePropertiesUsed()) {
            clonedPathPatternExpr.markPropertiesAsUsed();
        }

        // If we have decompositions, then we need to deep-copy these as well.
        for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
            VertexPatternExpr clonedDecompositionLeft = this.visit(decomposition.getLeftVertex(), null);
            VertexPatternExpr clonedDecompositionRight = this.visit(decomposition.getRightVertex(), null);
            EdgePatternExpr clonedDecomposition = this.visit(decomposition, null);
            clonedDecompositionLeft.setParentPathExpr(clonedPathPatternExpr);
            clonedDecompositionRight.setParentPathExpr(clonedPathPatternExpr);
            clonedDecomposition.setParentPathExpr(clonedPathPatternExpr);
            clonedPathPatternExpr.addDecomposition(clonedDecomposition);
        }
        return clonedPathPatternExpr;
    }

    @Override
    public GraphConstructor visit(GraphConstructor gc, Void arg) throws CompilationException {
        List<VertexConstructor> clonedVertexConstructors = new ArrayList<>();
        for (VertexConstructor vertexElement : gc.getVertexElements()) {
            clonedVertexConstructors.add(visit(vertexElement, arg));
        }
        List<EdgeConstructor> clonedEdgeConstructors = new ArrayList<>();
        for (EdgeConstructor edgeElement : gc.getEdgeElements()) {
            clonedEdgeConstructors.add(visit(edgeElement, arg));
        }
        GraphConstructor clonedGraphConstructor =
                new GraphConstructor(clonedVertexConstructors, clonedEdgeConstructors);
        clonedGraphConstructor.setSourceLocation(gc.getSourceLocation());
        return clonedGraphConstructor;
    }

    @Override
    public EdgeConstructor visit(EdgeConstructor ee, Void arg) throws CompilationException {
        List<Integer> clonedSourceKeySourceIndicators = new ArrayList<>(ee.getSourceKeySourceIndicators());
        List<Integer> clonedDestinationKeySourceIndicators = new ArrayList<>(ee.getDestinationKeySourceIndicators());
        List<List<String>> clonedSourceKeyFields = new ArrayList<>();
        for (List<String> sourceKeyField : ee.getSourceKeyFields()) {
            clonedSourceKeyFields.add(new ArrayList<>(sourceKeyField));
        }
        List<List<String>> clonedDestinationKeyFields = new ArrayList<>();
        for (List<String> destinationKeyField : ee.getDestinationKeyFields()) {
            clonedDestinationKeyFields.add(new ArrayList<>(destinationKeyField));
        }
        Expression clonedExpression = (Expression) ee.getExpression().accept(this, arg);
        EdgeConstructor clonedEdgeConstructor = new EdgeConstructor(ee.getEdgeLabel(), ee.getDestinationLabel(),
                ee.getSourceLabel(), clonedDestinationKeyFields, clonedDestinationKeySourceIndicators,
                clonedSourceKeyFields, clonedSourceKeySourceIndicators, clonedExpression, ee.getDefinition());
        clonedEdgeConstructor.setSourceLocation(ee.getSourceLocation());
        return ee;
    }

    @Override
    public VertexConstructor visit(VertexConstructor ve, Void arg) throws CompilationException {
        List<Integer> clonedSourceIndicators = new ArrayList<>(ve.getPrimaryKeySourceIndicators());
        List<List<String>> clonedKeyFields = new ArrayList<>();
        for (List<String> primaryKeyField : ve.getPrimaryKeyFields()) {
            clonedKeyFields.add(new ArrayList<>(primaryKeyField));
        }
        Expression clonedExpression = (Expression) ve.getExpression().accept(this, arg);
        VertexConstructor clonedVertexConstructor = new VertexConstructor(ve.getLabel(), clonedKeyFields,
                clonedSourceIndicators, clonedExpression, ve.getDefinition());
        clonedVertexConstructor.setSourceLocation(ve.getSourceLocation());
        return ve;
    }

    // We can safely ignore the statements below, we will not encounter them in queries.
    @Override
    public ILangExpression visit(CreateGraphStatement cgs, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphElementDeclaration ged, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public ILangExpression visit(GraphDropStatement gds, Void arg) throws CompilationException {
        return null;
    }
}
