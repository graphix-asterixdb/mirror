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

import static org.apache.asterix.graphix.extension.GraphixMetadataExtension.getGraph;
import static org.apache.asterix.graphix.lang.parser.GraphElementBodyParser.parse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.resolve.ISuperPatternResolver;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixScopingVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Schema;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Populate the {@link GraphElementDeclaration} of each {@link IPatternConstruct}. We assume that our graph elements
 * are properly labeled at this point (i.e. some instance of {@link ISuperPatternResolver} must run before this).
 */
public class ElementDeclarationVisitor extends AbstractGraphixScopingVisitor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IWarningCollector warningCollector;
    private final MetadataProvider metadataProvider;
    private final GraphixParserFactory parserFactory;
    private final GraphixDeepCopyVisitor deepCopyVisitor;

    private final Map<IElementIdentifier, List<IPatternConstruct>> identifierMap;
    private GraphIdentifier workingGraphIdentifier = null;

    public ElementDeclarationVisitor(GraphixRewritingContext rewritingContext, GraphixParserFactory parserFactory) {
        super(rewritingContext);
        this.parserFactory = Objects.requireNonNull(parserFactory);
        this.warningCollector = rewritingContext.getWarningCollector();
        this.metadataProvider = rewritingContext.getMetadataProvider();
        this.deepCopyVisitor = new GraphixDeepCopyVisitor();
        this.identifierMap = new LinkedHashMap<>();
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        workingGraphIdentifier = fgt.createGraphIdentifier(metadataProvider);
        scopeChecker.createNewScope();
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            matchClause.accept(this, fgt);
        }

        // After visiting all of our graph elements, attach our element bodies.
        GraphConstructor graphConstructor = getGraphConstructor(workingGraphIdentifier);
        if (graphConstructor == null) {
            DataverseName dataverseName = fgt.getDataverseName();
            if (dataverseName == null) {
                dataverseName = metadataProvider.getDefaultDataverseName();
            }
            Identifier graphName = fgt.getGraphName();

            // Load this from our metadata.
            Graph graphFromMetadata;
            try {
                MetadataTransactionContext metadataTxnContext = metadataProvider.getMetadataTxnContext();
                graphFromMetadata = getGraph(metadataTxnContext, dataverseName, graphName.getValue());
                if (graphFromMetadata == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                        "Graph " + graphName.getValue() + " does not exist.");
            }
            for (Vertex vertex : graphFromMetadata.getGraphSchema().getVertices()) {
                GraphElementDeclaration vertexDeclaration = parse(vertex, parserFactory, warningCollector);
                identifierMap.getOrDefault(vertex.getIdentifier(), List.of()).stream().map(v -> (VertexPatternExpr) v)
                        .forEach(v -> updateVertexWithInfo(v, vertex, vertexDeclaration));
            }
            for (Edge edge : graphFromMetadata.getGraphSchema().getEdges()) {
                GraphElementDeclaration edgeDeclaration = parse(edge, parserFactory, warningCollector);
                identifierMap.getOrDefault(edge.getIdentifier(), List.of()).stream().map(e -> (EdgePatternExpr) e)
                        .forEach(e -> updateEdgeWithInfo(e, edge, edgeDeclaration));
            }
        } else {
            Schema.Builder builder = new Schema.Builder(workingGraphIdentifier);
            for (VertexConstructor vertex : graphConstructor.getVertexElements()) {
                Vertex schemaVertex = builder.addVertex(vertex);
                VertexIdentifier vertexIdentifier = new VertexIdentifier(workingGraphIdentifier, vertex.getLabel());
                Expression bodyClone = (Expression) vertex.getExpression().accept(deepCopyVisitor, null);
                GraphElementDeclaration declaration = new GraphElementDeclaration(vertexIdentifier, bodyClone, false);
                identifierMap.getOrDefault(vertexIdentifier, List.of()).stream().map(v -> (VertexPatternExpr) v)
                        .forEach(v -> updateVertexWithInfo(v, schemaVertex, declaration));
            }
            for (EdgeConstructor edge : graphConstructor.getEdgeElements()) {
                Edge schemaEdge = builder.addEdge(edge);
                EdgeIdentifier edgeIdentifier = new EdgeIdentifier(workingGraphIdentifier, edge.getSourceLabel(),
                        edge.getEdgeLabel(), edge.getDestinationLabel());
                Expression bodyClone = (Expression) edge.getExpression().accept(deepCopyVisitor, null);
                GraphElementDeclaration declaration = new GraphElementDeclaration(edgeIdentifier, bodyClone, false);
                identifierMap.getOrDefault(edgeIdentifier, List.of()).stream().map(e -> (EdgePatternExpr) e)
                        .forEach(e -> updateEdgeWithInfo(e, schemaEdge, declaration));
            }
        }

        // Visit any correlated clauses we may have.
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        if (vertexDescriptor.getLabels().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vpe.getSourceLocation(),
                    "VertexPatternExpr found without labels. Elements should have been resolved earlier.");
        }
        for (ElementLabel label : vertexDescriptor.getLabels()) {
            VertexIdentifier identifier = new VertexIdentifier(workingGraphIdentifier, label);
            identifierMap.putIfAbsent(identifier, new ArrayList<>());
            identifierMap.get(identifier).add(vpe);
        }
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        if (edgeDescriptor.getLabels().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, epe.getSourceLocation(),
                    "EdgePatternExpr found without labels. Elements should have been resolved earlier.");
        }

        // We need to differentiate between edge labels of the same name, using the terminal vertices.
        VertexDescriptor leftDescriptor = epe.getLeftVertex().getVertexDescriptor();
        VertexDescriptor rightDescriptor = epe.getRightVertex().getVertexDescriptor();
        for (ElementLabel leftLabel : leftDescriptor.getLabels()) {
            for (ElementLabel rightLabel : rightDescriptor.getLabels()) {
                for (ElementLabel edgeLabel : edgeDescriptor.getLabels()) {
                    // Note: this may generate non-conforming edge identifiers.
                    if (edgeDescriptor.getElementDirection() != ElementDirection.RIGHT_TO_LEFT) {
                        EdgeIdentifier edgeIdentifier =
                                new EdgeIdentifier(workingGraphIdentifier, leftLabel, edgeLabel, rightLabel);
                        identifierMap.putIfAbsent(edgeIdentifier, new ArrayList<>());
                        identifierMap.get(edgeIdentifier).add(epe);
                    }
                    if (edgeDescriptor.getElementDirection() != ElementDirection.LEFT_TO_RIGHT) {
                        EdgeIdentifier edgeIdentifier =
                                new EdgeIdentifier(workingGraphIdentifier, rightLabel, edgeLabel, leftLabel);
                        identifierMap.putIfAbsent(edgeIdentifier, new ArrayList<>());
                        identifierMap.get(edgeIdentifier).add(epe);
                    }
                }
            }
        }
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
            decomposition.getLeftVertex().accept(this, arg);
            decomposition.accept(this, arg);
            decomposition.getRightVertex().accept(this, arg);
        }
        return super.visit(ppe, arg);
    }

    @Override
    public Expression visit(VariableExpr ve, ILangExpression arg) throws CompilationException {
        return ve;
    }

    private void updateVertexWithInfo(VertexPatternExpr vertexPattern, Vertex vertexInfo,
            GraphElementDeclaration vertexDeclaration) {
        if (LOGGER.isDebugEnabled()) {
            SourceLocation sourceLocation = vertexPattern.getSourceLocation();
            String vertexLine = String.format("line %s", sourceLocation.getLine());
            String vertexColumn = String.format("column %s", sourceLocation.getColumn());
            String vertexLocation = String.format("(%s, %s)", vertexLine, vertexColumn);
            String vertexVar = vertexPattern.getVertexDescriptor().getVariableExpr().toString();
            String logMessage = "Vertex {} {} matches the definition:\n\t{}";
            LOGGER.debug(logMessage, vertexVar, vertexLocation, vertexInfo);
        }
        vertexPattern.addVertexDeclaration(vertexDeclaration);
        vertexPattern.addVertexInfo(vertexInfo);
    }

    private void updateEdgeWithInfo(EdgePatternExpr edgePattern, Edge edgeInfo,
            GraphElementDeclaration edgeDeclaration) {
        if (LOGGER.isDebugEnabled()) {
            SourceLocation sourceLocation = edgePattern.getSourceLocation();
            String edgeLine = String.format("line %s", sourceLocation.getLine());
            String edgeColumn = String.format("column %s", sourceLocation.getColumn());
            String edgeLocation = String.format("(%s, %s)", edgeLine, edgeColumn);
            String edgeVar = edgePattern.getEdgeDescriptor().getVariableExpr().toString();
            String logMessage = "Edge {} {} matches the body:\n\t{}";
            LOGGER.debug(logMessage, edgeVar, edgeLocation, edgeInfo);
        }
        edgePattern.addEdgeDeclaration(edgeDeclaration);
        edgePattern.addEdgeInfo(edgeInfo);
    }
}
