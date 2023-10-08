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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixScopingVisitor;
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
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * A pre-rewrite pass to validate / raise warnings about our user query.
 * <p>
 * We validate the following:
 * <ul>
 *  <li>An connection (edge or path) variable is not defined more than once. (e.g. (u)-[e]-(v), (w)-[e]-(y))</li>
 *  <li>A vertex variable is not defined more than once with labels. (e.g. (u:User)-[e]-(v), (u:User)-[]-(:Review))</li>
 *  <li>An edge / path label exists in the context of the edge's {@link FromGraphTerm}.</li>
 *  <li>A vertex label exists in the context of the vertex's {@link FromGraphTerm}.</li>
 *  <li>The minimum hops and maximum hops is greater than or equal to zero.</li>
 *  <li>The maximum hops of a path is greater than or equal to the minimum hops of the same path.</li>
 *  <li>An anonymous / declared graph passes the same validation that a named graph does.</li>
 *  <li>That variables in an element filter expression do not reference other previously defined graph-elements.</li>
 * </ul>
 */
public class PreRewriteCheckVisitor extends AbstractGraphixScopingVisitor {
    private final MetadataProvider metadataProvider;

    // Build new environments on each FROM-GRAPH-TERM visit.
    private static class Environment {
        private final Set<ElementLabel> elementLabels = new HashSet<>();
        private final Set<ElementLabel> edgeLabels = new HashSet<>();
        private final Set<Identifier> connectionVariables = new HashSet<>();
        private final Set<Identifier> allElementVariables = new HashSet<>();
        private final Set<Identifier> vertexVariablesWithLabels = new HashSet<>();
    }

    private final Map<ILangExpression, Environment> environmentMap = new HashMap<>();

    public PreRewriteCheckVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
    }

    @Override
    public Expression visit(LetGraphClause lgc, ILangExpression arg) throws CompilationException {
        lgc.getGraphConstructor().accept(this, lgc);
        return super.visit(lgc, arg);
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        GraphIdentifier graphIdentifier = ((LetGraphClause) arg).createGraphIdentifier(metadataProvider);
        Schema.Builder schemaBuilder = new Schema.Builder(graphIdentifier);

        // Perform the same validation we do for managed graphs-- but don't build the schema object.
        for (VertexConstructor vertex : gc.getVertexElements()) {
            schemaBuilder.addVertex(vertex);
            if (schemaBuilder.getLastError() == Schema.Builder.Error.VERTEX_LABEL_CONFLICT) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertex.getSourceLocation(),
                        "Conflicting vertex label found: " + vertex.getLabel());

            } else if (schemaBuilder.getLastError() != Schema.Builder.Error.NO_ERROR) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vertex.getSourceLocation(),
                        "Constructor vertex was not returned, but the error is not a conflicting vertex label!");
            }
        }
        for (EdgeConstructor edge : gc.getEdgeElements()) {
            schemaBuilder.addEdge(edge);
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    continue;

                case SOURCE_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Source vertex " + edge.getSourceLabel() + " not found in the edge " + edge.getEdgeLabel()
                                    + ".");

                case DESTINATION_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Destination vertex " + edge.getDestinationLabel() + " not found in the edge "
                                    + edge.getEdgeLabel() + ".");

                case EDGE_LABEL_CONFLICT:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Conflicting edge label found: " + edge.getEdgeLabel());

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, edge.getSourceLocation(),
                            "Edge constructor was not returned, and an unexpected error encountered");
            }
        }
        return null;
    }

    // TODO (GLENN): Raise a warning about disconnected patterns.
    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        environmentMap.put(fgt, new Environment());
        scopeChecker.createNewScope();

        // Establish the vertex and edge labels associated with this FROM-GRAPH-TERM.
        DataverseName defaultDataverse = metadataProvider.getDefaultDataverseName();
        DataverseName dataverseName = Objects.requireNonNullElse(fgt.getDataverseName(), defaultDataverse);
        Identifier graphName = fgt.getGraphName();

        // First, try to see if this graph has been defined in the scope of our query.
        GraphIdentifier graphIdentifier = fgt.createGraphIdentifier(metadataProvider);
        GraphConstructor graphConstructor = getGraphConstructor(graphIdentifier);
        if (graphConstructor == null) {
            try {
                // We could not find our graph in-scope. Fetch the graph from our metadata.
                MetadataTransactionContext metadataTxnContext = metadataProvider.getMetadataTxnContext();
                Graph graphFromMetadata = getGraph(metadataTxnContext, dataverseName, graphName.getValue());
                if (graphFromMetadata == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");

                } else {
                    graphFromMetadata.getGraphSchema().getVertices().stream().map(Vertex::getLabel)
                            .forEach(environmentMap.get(fgt).elementLabels::add);
                    graphFromMetadata.getGraphSchema().getEdges().forEach(e -> {
                        environmentMap.get(fgt).elementLabels.add(e.getSourceLabel());
                        environmentMap.get(fgt).elementLabels.add(e.getDestinationLabel());
                        environmentMap.get(fgt).edgeLabels.add(e.getLabel());
                    });
                }

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                        "Graph " + graphName.getValue() + " does not exist.");
            }

        } else {
            // We have an unmanaged graph.
            graphConstructor.getVertexElements().stream().map(VertexConstructor::getLabel)
                    .forEach(environmentMap.get(fgt).elementLabels::add);
            graphConstructor.getEdgeElements().forEach(e -> {
                environmentMap.get(fgt).elementLabels.add(e.getSourceLabel());
                environmentMap.get(fgt).elementLabels.add(e.getDestinationLabel());
                environmentMap.get(fgt).edgeLabels.add(e.getEdgeLabel());
            });
        }

        // We need to pass our FROM-GRAPH-TERM to our MATCH-CLAUSE.
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            matchClause.accept(this, fgt);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        Environment environment = environmentMap.get(arg);
        for (ElementLabel label : vertexDescriptor.getLabels()) {
            if (environment.elementLabels.stream().noneMatch(e -> e.getLabelName().equals(label.getLabelName()))) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vpe.getSourceLocation(),
                        "Vertex label " + label + " does not exist in the given graph schema.");
            }
        }
        if (vertexDescriptor.getFilterExpr() != null) {
            vertexDescriptor.getFilterExpr().accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
                    if (environment.allElementVariables.contains(varExpr.getVar())) {
                        SourceLocation sourceLocation = vertexDescriptor.getFilterExpr().getSourceLocation();
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation,
                                "Cannot reference other graph elements in a filter expression! Consider putting your "
                                        + "condition in a WHERE clause.");
                    }
                    return super.visit(varExpr, arg);
                }
            }, null);
        }
        if (vertexDescriptor.getVariableExpr() != null && !vertexDescriptor.getLabels().isEmpty()) {
            Identifier vertexIdentifier = vertexDescriptor.getVariableExpr().getVar();
            if (environment.vertexVariablesWithLabels.contains(vertexIdentifier)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vpe.getSourceLocation(),
                        "Vertex " + vertexIdentifier + " defined with a label more than once. Labels can only be "
                                + "bound to vertices once.");
            }
            environment.vertexVariablesWithLabels.add(vertexIdentifier);
            environment.allElementVariables.add(vertexIdentifier);
        }
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        Environment environment = environmentMap.get(arg);
        if (environment.edgeLabels.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, epe.getSourceLocation(),
                    "Query edge given, but no edge is defined in the schema.");
        }
        for (ElementLabel label : edgeDescriptor.getLabels()) {
            if (environment.edgeLabels.stream().noneMatch(e -> e.getLabelName().equals(label.getLabelName()))) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, epe.getSourceLocation(),
                        "Edge label " + label + " does not exist in the given graph schema.");
            }
        }
        if (edgeDescriptor.getFilterExpr() != null) {
            edgeDescriptor.getFilterExpr().accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
                    if (environment.allElementVariables.contains(varExpr.getVar())) {
                        SourceLocation sourceLocation = edgeDescriptor.getFilterExpr().getSourceLocation();
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation,
                                "Cannot reference other graph elements in a filter expression! Consider putting your "
                                        + "condition in a WHERE clause.");
                    }
                    return super.visit(varExpr, arg);
                }
            }, null);
        }
        if (edgeDescriptor.getVariableExpr() != null) {
            Identifier edgeIdentifier = edgeDescriptor.getVariableExpr().getVar();
            if (environment.connectionVariables.contains(edgeIdentifier)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, epe.getSourceLocation(),
                        "Connection " + edgeIdentifier + " defined more than once. Paths / edges can only"
                                + " connect two vertices.");
            }
            environment.connectionVariables.add(edgeIdentifier);
            environment.allElementVariables.add(edgeIdentifier);
        }
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        Integer minimumHops = pathDescriptor.getMinimumHops();
        Integer maximumHops = pathDescriptor.getMaximumHops();
        if ((maximumHops != null && maximumHops < 0) || minimumHops < 0) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                    "Paths cannot have a hop length less than 0.");

        } else if (maximumHops != null && maximumHops < minimumHops) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                    "Paths cannot have a maximum hop length (" + maximumHops + ") less than the minimum hop length ("
                            + minimumHops + ").");
        }

        // Perform the same checks as we do with edges.
        Environment environment = environmentMap.get(arg);
        if (environment.edgeLabels.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                    "Query path given, but no edge is defined in the schema.");
        }
        for (ElementLabel label : pathDescriptor.getLabels()) {
            if (environment.edgeLabels.stream().noneMatch(e -> e.getLabelName().equals(label.getLabelName()))) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                        "Edge label " + label + " does not exist in the given graph schema.");
            }
        }
        if (pathDescriptor.getVariableExpr() != null) {
            Identifier pathIdentifier = pathDescriptor.getVariableExpr().getVar();
            if (environment.connectionVariables.contains(pathIdentifier)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, ppe.getSourceLocation(),
                        "Connection " + pathIdentifier + " defined more than once. Paths / edges can only"
                                + " connect two vertices.");
            }
            environment.connectionVariables.add(pathIdentifier);
            environment.allElementVariables.add(pathIdentifier);
        }
        return super.visit(ppe, arg);
    }

    @Override
    public Expression visit(VariableExpr ve, ILangExpression arg) throws CompilationException {
        return ve;
    }
}
