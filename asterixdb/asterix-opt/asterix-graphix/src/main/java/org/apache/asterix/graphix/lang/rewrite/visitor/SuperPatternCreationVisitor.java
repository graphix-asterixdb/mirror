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
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.resolve.pattern.SuperPattern;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixScopingVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Aggregate all query patterns into a single list of patterns (contained in {@link SuperPattern}) across our AST,
 * grouped by the graph being queried. We return a maximal cover here.
 */
public class SuperPatternCreationVisitor extends AbstractGraphixScopingVisitor
        implements Iterable<Pair<GraphIdentifier, SuperPattern>> {
    private final List<NamedSuperPattern> selectBlockSuperPatterns;
    private final Deque<NamedSuperPattern> superPatternStack;
    protected final MetadataProvider metadataProvider;

    private final static class NamedSuperPattern {
        private final GraphIdentifier graphIdentifier;
        private final SuperPattern superPattern;

        private NamedSuperPattern(GraphIdentifier graphIdentifier, SuperPattern superPattern) {
            this.graphIdentifier = graphIdentifier;
            this.superPattern = superPattern;
        }
    }

    public SuperPatternCreationVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
        this.selectBlockSuperPatterns = new ArrayList<>();
        this.superPatternStack = new ArrayDeque<>();
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        int beforeVisitSize = superPatternStack.size();
        super.visit(sb, arg);

        // Remove all super-patterns that we have introduced from this visit.
        int afterVisitSize = superPatternStack.size();
        for (int i = 0; i < afterVisitSize - beforeVisitSize; i++) {
            NamedSuperPattern namedSuperPattern = superPatternStack.removeLast();
            if (namedSuperPattern.graphIdentifier != null) {
                selectBlockSuperPatterns.add(namedSuperPattern);
            }
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        // Build a new super-pattern. This will contain all patterns currently in scope.
        GraphIdentifier graphIdentifier = fgt.createGraphIdentifier(metadataProvider);
        SuperPattern superPattern =
                superPatternStack.stream().filter(p -> Objects.equals(p.graphIdentifier, graphIdentifier))
                        .map(p -> p.superPattern).reduce(new SuperPattern(), (p1, p2) -> {
                            for (IPatternConstruct patternConstruct : p2) {
                                p1.addPatternConstruct(patternConstruct);
                            }
                            return p1;
                        });

        // Collect the vertices and edges in this FROM-GRAPH-TERM.
        scopeChecker.createNewScope();
        superPatternStack.addLast(new NamedSuperPattern(graphIdentifier, superPattern));
        for (MatchClause matchClause : fgt.getMatchClauses()) {
            matchClause.accept(this, arg);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fgt.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        superPatternStack.getLast().superPattern.addPatternConstruct(vpe);
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        superPatternStack.getLast().superPattern.addPatternConstruct(epe);
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        superPatternStack.getLast().superPattern.addPatternConstruct(ppe);
        return super.visit(ppe, arg);
    }

    @Override
    public Expression visit(VariableExpr ve, ILangExpression arg) throws CompilationException {
        return ve;
    }

    @Override
    public Iterator<Pair<GraphIdentifier, SuperPattern>> iterator() {
        // We will take a greedy approach to finding a maximal cover.
        Set<SuperPattern> visitedSuperPatterns = new HashSet<>();
        return selectBlockSuperPatterns.stream()
                .sorted((p1, p2) -> Integer.compare(p2.superPattern.size(), p1.superPattern.size())).filter(p -> {
                    for (SuperPattern visitedSuperPattern : visitedSuperPatterns) {
                        if (visitedSuperPattern.contains(p.superPattern)) {
                            return false;
                        }
                    }
                    visitedSuperPatterns.add(p.superPattern);
                    return true;
                }).map(p -> new Pair<>(p.graphIdentifier, p.superPattern)).iterator();
    }
}
