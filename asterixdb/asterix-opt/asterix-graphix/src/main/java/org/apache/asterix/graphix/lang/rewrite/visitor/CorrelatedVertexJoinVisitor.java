/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.scope.ScopedCollection;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Search for Graphix sub-queries that have 'free' {@link VertexPatternExpr}s, and see if we can explicitly JOIN them
 * with a {@link VertexPatternExpr} in a parent query that refers to the same graph. Note that this will <b>not</b>
 * rewrite other usages of the correlated vertex (e.g. in the {@link WhereClause}), meaning that this rewrite will
 * force other correlated clauses to use the outer vertex instead.
 * <p>
 * We assume that the following rewrites have already been invoked:
 * <ol>
 *   <li>{@link PreRewriteCheckVisitor}</li>
 *   <li>{@link FillStartingUnknownsVisitor}</li>
 * </ol>
 */
public class CorrelatedVertexJoinVisitor extends ExpressionScopingVisitor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final GraphixRewritingContext graphixRewritingContext;
    private final ScopedCollection<VertexContext> contextCollection;
    private final Deque<Environment> correlationEnvironmentStack;

    private final static class VertexContext {
        private final VertexPatternExpr vertexPatternExpr;
        private final GraphIdentifier graphIdentifier;

        public VertexContext(VertexPatternExpr vertexPatternExpr, GraphIdentifier graphIdentifier) {
            this.vertexPatternExpr = vertexPatternExpr;
            this.graphIdentifier = graphIdentifier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VertexContext)) {
                return false;
            }
            VertexContext that = (VertexContext) o;
            return Objects.equals(vertexPatternExpr, that.vertexPatternExpr)
                    && Objects.equals(graphIdentifier, that.graphIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertexPatternExpr, graphIdentifier);
        }
    }

    private final static class Environment {
        private Map<VariableExpr, VariableExpr> variableMap;
        private GraphIdentifier graphIdentifier;
    }

    public CorrelatedVertexJoinVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
        this.graphixRewritingContext = graphixRewritingContext;
        this.correlationEnvironmentStack = new ArrayDeque<>();
        this.contextCollection = new ScopedCollection<>();
    }

    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        Environment environment = new Environment();
        environment.variableMap = new HashMap<>();
        correlationEnvironmentStack.addLast(environment);
        super.visit(sb, arg);
        correlationEnvironmentStack.removeLast();
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        Environment environment = correlationEnvironmentStack.getLast();
        environment.graphIdentifier = fgt.createGraphIdentifier(metadataProvider);
        return super.visit(fgt, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        Environment environment = correlationEnvironmentStack.getLast();
        Scope currentScope = scopeChecker.getCurrentScope();
        Scope parentScope = currentScope.getParentScope();

        // Find any vertices that exist in our **parent** scope.
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
        Optional<VertexPatternExpr> matchingVertex = contextCollection.getElementsInScope(parentScope).stream()
                .filter(c -> Objects.equals(c.graphIdentifier, environment.graphIdentifier))
                .map(v -> v.vertexPatternExpr)
                .filter(v -> v.getVertexDescriptor().getVariableExpr().equals(vertexVariable)).findAny();
        if (matchingVertex.isPresent()) {
            // We have found a vertex that references an outer vertex.
            if (LOGGER.isDebugEnabled()) {
                SourceLocation sourceLocation = vpe.getSourceLocation();
                String vertexLine = String.format("line %s", sourceLocation.getLine());
                String vertexColumn = String.format("column %s", sourceLocation.getColumn());
                String vertexLocation = String.format("(%s, %s)", vertexLine, vertexColumn);
                LOGGER.debug("Found correlated vertex {} at {}.", vertexVariable, vertexLocation);
            }
            if (environment.variableMap.containsKey(vertexVariable)) {
                // We have replaced this variable already. Update the reference.
                VariableExpr remappedVertexVariable = environment.variableMap.get(vertexVariable);
                vertexDescriptor.setVariableExpr(new VariableExpr(remappedVertexVariable.getVar()));
                vpe.setParentVertexExpr(matchingVertex.get());
                return super.visit(vpe, arg);
            }

            // Replace the current vertex variable with a copy.
            VariableExpr newVariable = graphixRewritingContext.getGraphixVariableCopy(vertexVariable);
            vertexDescriptor.setVariableExpr(newVariable);
            environment.variableMap.put(vertexVariable, newVariable);
            vpe.setParentVertexExpr(matchingVertex.get());

        } else {
            VertexContext newContext = new VertexContext(vpe, environment.graphIdentifier);
            contextCollection.addElement(newContext, vertexVariable.getVar(), currentScope);
        }
        return super.visit(vpe, arg);
    }
}
