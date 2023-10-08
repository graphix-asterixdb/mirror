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
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.scope.ScopedCollection;
import org.apache.asterix.graphix.lang.rewrite.util.ExpressionScopeUtil;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Search for {@link VertexPatternExpr} instances where nothing other than the primary key of the vertex body is
 * required. This allows us to potentially not introduce the body of the "dead" vertex, and replace all references its
 * primary key with the foreign key field(s) of the edge. A {@link VertexPatternExpr} is marked as "live" if...
 * <ol>
 *   <li>...there is a reference to the corresponding {@link VariableExpr} of a {@link VertexPatternExpr} OR...</li>
 *   <li>...all references to the corresponding {@link VariableExpr} of a {@link VertexPatternExpr} are captured using
 *   the primary key fields of the {@link VertexPatternExpr} OR...</li>
 *   <li>...if the corresponding {@link VariableExpr} of a {@link PathPatternExpr} is referenced.</li>
 * </ol>
 * Note that this rule <b>does not</b> perform any form of equivalence analysis, and therefore queries that rebind the
 * corresponding {@link VariableExpr} are treated as usages of the {@link VertexPatternExpr} body (or
 * {@link PathPatternExpr}).
 * <p>
 * We assume that the following rewrites have already been invoked:
 * <ol>
 *   <li>{@link PreRewriteCheckVisitor}</li>
 *   <li>{@link FillStartingUnknownsVisitor}</li>
 *   <li>{@link ElementDeclarationVisitor}</li>
 *   <li>{@link FunctionResolutionVisitor}</li>
 * </ol>
 */
public class LiveVertexMarkingVisitor extends ExpressionScopingVisitor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ScopedCollection<VertexPatternExpr> vertexCollection = new ScopedCollection<>();
    private final ScopedCollection<PathPatternExpr> pathCollection = new ScopedCollection<>();
    private final Map<VariableExpr, Scope> groupByScopeMap = new LinkedHashMap<>();
    private final Deque<Identifier> fieldAccessIdentifierStack = new ArrayDeque<>();

    // By default, we will mark our vertices / paths as used.
    private final ExpressionFoundCallback expressionFoundCallback = new ExpressionFoundCallback() {
        @Override
        public <T extends AbstractExpression> void accept(T expr) {
            if (expr instanceof VertexPatternExpr) {
                ((VertexPatternExpr) expr).markPropertiesAsUsed();

            } else if (expr instanceof PathPatternExpr) {
                ((PathPatternExpr) expr).markPropertiesAsUsed();

            } else {
                throw new IllegalArgumentException("Only vertices and paths are expected!");
            }
        }
    };

    public LiveVertexMarkingVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
    }

    @Override
    public Expression visit(SelectExpression se, ILangExpression arg) throws CompilationException {
        super.visit(se, arg);
        if (LOGGER.isDebugEnabled()) {
            se.accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
                    if (!vpe.arePropertiesUsed()) {
                        SourceLocation sourceLocation = vpe.getSourceLocation();
                        String vertexLine = String.format("line %s", sourceLocation.getLine());
                        String vertexColumn = String.format("column %s", sourceLocation.getColumn());
                        String vertexLocation = String.format("(%s, %s)", vertexLine, vertexColumn);
                        String logMessage = "Vertex {} {} does not require its properties.";
                        LOGGER.debug(logMessage, vpe, vertexLocation);
                    }
                    return super.visit(vpe, arg);
                }

                @Override
                public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
                    for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
                        decomposition.getLeftVertex().accept(this, arg);
                        decomposition.getRightVertex().accept(this, arg);
                    }
                    return super.visit(ppe, arg);
                }
            }, null);
        }
        return se;
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        Identifier pathSymbol = ppe.getPathDescriptor().getVariableExpr().getVar();
        pathCollection.addElement(ppe, pathSymbol, scopeChecker.getCurrentScope());
        return super.visit(ppe, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        Identifier vertexSymbol = vpe.getVertexDescriptor().getVariableExpr().getVar();
        vertexCollection.addElement(vpe, vertexSymbol, scopeChecker.getCurrentScope());
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(VariableExpr v, ILangExpression arg) throws CompilationException {
        vertexCollection.getElementsInScope(scopeChecker.getCurrentScope()).stream()
                .filter(vv -> Objects.equals(vv.getVertexDescriptor().getVariableExpr().getVar(), v.getVar()))
                .forEach(expressionFoundCallback::accept);
        pathCollection.getElementsInScope(scopeChecker.getCurrentScope()).stream()
                .filter(vv -> Objects.equals(vv.getPathDescriptor().getVariableExpr().getVar(), v.getVar()))
                .forEach(expressionFoundCallback::accept);
        if (groupByScopeMap.containsKey(v)) {
            vertexCollection.getElementsInScope(groupByScopeMap.get(v)).forEach(expressionFoundCallback::accept);
            pathCollection.getElementsInScope(groupByScopeMap.get(v)).forEach(expressionFoundCallback::accept);
        }
        return super.visit(v, arg);
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
        Scope currentScope = scopeChecker.getCurrentScope();
        switch (fa.getExpr().getKind()) {
            case VARIABLE_EXPRESSION:
                VariableExpr variableExpr = (VariableExpr) fa.getExpr();
                List<VertexPatternExpr> referencedVertices =
                        vertexCollection.getElementsInScope(currentScope).stream().filter(v -> {
                            VarIdentifier vertexId = v.getVertexDescriptor().getVariableExpr().getVar();
                            return Objects.equals(vertexId, variableExpr.getVar());
                        }).collect(Collectors.toList());
                if (referencedVertices.isEmpty() && groupByScopeMap.containsKey(variableExpr)) {
                    String internalName = SqlppVariableUtil.toInternalVariableName(fa.getIdent().getValue());
                    VariableExpr candidateVariableExpr = new VariableExpr(new VarIdentifier(internalName));
                    Scope groupByScope = groupByScopeMap.get(variableExpr);
                    referencedVertices = vertexCollection.getElementsInScope(groupByScope).stream().filter(v -> {
                        VarIdentifier vertexId = v.getVertexDescriptor().getVariableExpr().getVar();
                        return Objects.equals(vertexId, candidateVariableExpr.getVar());
                    }).collect(Collectors.toList());
                    if (referencedVertices.isEmpty()) {
                        // We will not visit our GROUP-AS variable here.
                        return fa;
                    }
                } else if (referencedVertices.isEmpty()) {
                    return super.visit(fa, fa);
                }

                // We have a field access on a vertex. Is this access a non-primary key field?
                List<String> fieldAccessIdentifiers = new ArrayList<>();
                if (!groupByScopeMap.containsKey(variableExpr)) {
                    fieldAccessIdentifiers.add(fa.getIdent().getValue());
                }
                fieldAccessIdentifierStack.forEach(f -> fieldAccessIdentifiers.add(f.getValue()));
                for (VertexPatternExpr referencedVertex : referencedVertices) {
                    boolean hasMatchingField = referencedVertex.getVertexDescriptor().getLabels().stream()
                            .map(referencedVertex::getKeyFieldNames).anyMatch(v -> {
                                for (List<String> primaryKeyField : v) {
                                    if (primaryKeyField.equals(fieldAccessIdentifiers)) {
                                        return true;
                                    }
                                }
                                return false;
                            });
                    if (!hasMatchingField) {
                        expressionFoundCallback.accept(referencedVertex);
                    }
                }
                return fa;

            case FIELD_ACCESSOR_EXPRESSION:
                // We have a nested field access. Recurse and check if this accesses our vertex.
                if (ExpressionScopeUtil.isForGraphixScopeChecking(fa, arg)) {
                    fieldAccessIdentifierStack.push(fa.getIdent());
                    fa.getExpr().accept(this, arg);
                    fieldAccessIdentifierStack.pop();
                }
                return fa;

            default:
                return super.visit(fa, fa);
        }
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws CompilationException {
        if (gc.hasGroupVar()) {
            groupByScopeMap.put(gc.getGroupVar(), scopeChecker.getCurrentScope());
        }

        // We will not visit our GROUP-FIELD list here.
        List<Pair<Expression, Identifier>> groupFieldList = gc.getGroupFieldList();
        gc.setGroupFieldList(List.of());
        super.visit(gc, arg);
        gc.setGroupFieldList(groupFieldList);
        return null;
    }

    @FunctionalInterface
    private interface ExpressionFoundCallback {
        <T extends AbstractExpression> void accept(T expr);
    }
}
