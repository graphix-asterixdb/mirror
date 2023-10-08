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
package org.apache.asterix.graphix.lang.rewrite.resolve;

import static org.apache.asterix.graphix.lang.rewrite.util.PatternExpansionUtil.expandElementDirection;
import static org.apache.asterix.graphix.lang.rewrite.util.PatternExpansionUtil.expandElementLabels;
import static org.apache.asterix.graphix.lang.rewrite.util.PatternExpansionUtil.explodePathPattern;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.resolve.pattern.SuperPattern;
import org.apache.asterix.graphix.lang.rewrite.resolve.schema.SchemaQuery;
import org.apache.asterix.graphix.lang.rewrite.resolve.schema.SchemaTable;
import org.apache.asterix.graphix.lang.rewrite.visitor.FillStartingUnknownsVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.struct.AbstractDescriptor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Given a collection of {@link IPatternConstruct}s, resolve as many unknown elements ({@link ElementLabel} and
 * {@link ElementDirection} fields) using information from sibling {@link IPatternConstruct}s.
 * <p>
 * We assume that each {@link IPatternConstruct} has a non-null {@link VariableExpr}, meaning that
 * {@link FillStartingUnknownsVisitor} should have been beforehand.
 */
public class ExhaustiveSearchResolver implements ISuperPatternResolver {
    private static final Logger LOGGER = LogManager.getLogger();

    // A SchemaTable instance is specific to a single graph.
    private final SchemaTable schemaKnowledgeTable;
    private final GraphixRewritingContext rewritingContext;

    public ExhaustiveSearchResolver(SchemaTable schemaKnowledgeTable, GraphixRewritingContext rewritingContext) {
        this.schemaKnowledgeTable = schemaKnowledgeTable;
        this.rewritingContext = rewritingContext;
    }

    private void expandVertexPattern(VertexPatternExpr unexpandedVertexPattern, Deque<SuperPattern> inputPatternGroups,
            Deque<SuperPattern> outputPatternGroups) throws CompilationException {
        VertexDescriptor vertexDescriptor = unexpandedVertexPattern.getVertexDescriptor();
        VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
        Set<ElementLabel> vertexLabels = vertexDescriptor.getLabels();

        // Gather all labels that we can possibly expand to.
        Set<ElementLabel> expandedLabelSet = expandElementLabels(vertexLabels, schemaKnowledgeTable.getVertexLabels());

        // Expand our pattern-groups.
        while (!inputPatternGroups.isEmpty()) {
            SuperPattern unexpandedPatternGroup = inputPatternGroups.pop();
            for (ElementLabel elementLabel : expandedLabelSet) {
                SuperPattern canonicalPatternGroup = new SuperPattern(unexpandedPatternGroup);
                for (IPatternConstruct canonicalPattern : canonicalPatternGroup) {
                    canonicalPattern.accept(new AbstractGraphixQueryVisitor() {
                        @Override
                        public Expression visit(VertexPatternExpr vpe, ILangExpression arg)
                                throws CompilationException {
                            VertexDescriptor canonicalVertexDescriptor = vpe.getVertexDescriptor();
                            if (canonicalVertexDescriptor.getVariableExpr().equals(vertexVariable)) {
                                canonicalVertexDescriptor.replaceLabels(new HashSet<>(List.of(elementLabel)));
                            }
                            return super.visit(vpe, arg);
                        }
                    }, null);
                }
                outputPatternGroups.push(unifyVertexDescriptors(canonicalPatternGroup));
            }
        }
    }

    private void expandEdgePattern(EdgePatternExpr unexpandedEdgePattern, Deque<SuperPattern> inputPatternGroups,
            Deque<SuperPattern> outputPatternGroups) throws CompilationException {
        EdgeDescriptor edgeDescriptor = unexpandedEdgePattern.getEdgeDescriptor();
        VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
        Set<ElementLabel> edgeLabels = edgeDescriptor.getLabels();

        // Gather all labels that we can possibly expand to.
        Set<ElementLabel> expandedLabelSet = expandElementLabels(edgeLabels, schemaKnowledgeTable.getEdgeLabels());

        // If we have an UNDIRECTED element, then we need to check for LEFT_TO_RIGHT and RIGHT_TO_LEFT element.
        Set<ElementDirection> expandedDirectionSet = expandElementDirection(edgeDescriptor.getElementDirection());

        // Expand our pattern-groups.
        while (!inputPatternGroups.isEmpty()) {
            SuperPattern unexpandedPatternGroup = inputPatternGroups.pop();
            for (ElementLabel elementLabel : expandedLabelSet) {
                for (ElementDirection elementDirection : expandedDirectionSet) {
                    SuperPattern canonicalPatternGroup = new SuperPattern(unexpandedPatternGroup);
                    for (IPatternConstruct canonicalPattern : canonicalPatternGroup) {
                        canonicalPattern.accept(new AbstractGraphixQueryVisitor() {
                            @Override
                            public Expression visit(EdgePatternExpr epe, ILangExpression arg)
                                    throws CompilationException {
                                EdgeDescriptor canonicalEdgeDescriptor = epe.getEdgeDescriptor();
                                if (canonicalEdgeDescriptor.getVariableExpr().equals(edgeVariable)) {
                                    canonicalEdgeDescriptor.replaceLabels(new HashSet<>(List.of(elementLabel)));
                                    canonicalEdgeDescriptor.setElementDirection(elementDirection);
                                }
                                return super.visit(epe, arg);
                            }
                        }, null);
                    }
                    outputPatternGroups.push(unifyVertexDescriptors(canonicalPatternGroup));
                }
            }
        }
    }

    private void expandPathPattern(PathPatternExpr unexpandedPathPattern, Deque<SuperPattern> inputPatternGroups,
            Deque<SuperPattern> outputPatternGroups) throws CompilationException {
        PathDescriptor pathDescriptor = unexpandedPathPattern.getPathDescriptor();
        Set<ElementLabel> pathLabels = pathDescriptor.getLabels();
        VariableExpr pathVariable = pathDescriptor.getVariableExpr();

        // Gather all labels that our edge and vertex can possibly expand to.
        Set<ElementLabel> expandedEdgeLabelSet = expandElementLabels(pathLabels, schemaKnowledgeTable.getEdgeLabels());

        // Gather all labels that our internal vertex can possibly expand to.
        Set<ElementLabel> expandedVertexLabelSet = new HashSet<>();
        schemaKnowledgeTable.stream().filter(e -> expandedEdgeLabelSet.contains(e.getEdgeLabel())).forEach(e -> {
            expandedVertexLabelSet.add(e.getSourceLabel());
            expandedVertexLabelSet.add(e.getDestinationLabel());
        });

        // If we have an UNDIRECTED element, then we need to check for LEFT_TO_RIGHT and RIGHT_TO_LEFT element.
        Set<ElementDirection> expandedDirectionSet = expandElementDirection(pathDescriptor.getElementDirection());

        // Determine the length of **expanded** path. For {N,M} w/ E labels... MIN(M, (1 + 2(E-1))).
        int minimumExpansionLength, expansionLength;
        Integer minimumHops = pathDescriptor.getMinimumHops();
        Integer maximumHops = pathDescriptor.getMaximumHops();
        if (Objects.equals(minimumHops, maximumHops) && minimumHops != null && minimumHops == 1) {
            // Special case: we have an edge disguised as a path.
            minimumExpansionLength = 1;
            expansionLength = 1;

        } else {
            int maximumExpansionLength = 1 + 2 * (expandedEdgeLabelSet.size() - 1);
            minimumExpansionLength = Objects.requireNonNullElse(minimumHops, 1);
            expansionLength = Objects.requireNonNullElse(maximumHops, maximumExpansionLength);
            if (minimumExpansionLength > expansionLength) {
                minimumExpansionLength = expansionLength;
            }
        }

        // Generate all possible paths, from minimumExpansionLength to expansionLength.
        List<QueryPatternExpr> candidatePaths = new ArrayList<>();
        for (int pathLength = minimumExpansionLength; pathLength <= expansionLength; pathLength++) {
            List<QueryPatternExpr> expandedPathPattern = explodePathPattern(unexpandedPathPattern, pathLength,
                    expandedEdgeLabelSet, expandedVertexLabelSet, expandedDirectionSet, rewritingContext);
            candidatePaths.addAll(expandedPathPattern);
        }

        // Expand our pattern-groups.
        while (!inputPatternGroups.isEmpty()) {
            SuperPattern unexpandedPatternGroup = inputPatternGroups.pop();
            for (QueryPatternExpr candidatePath : candidatePaths) {
                SuperPattern canonicalPatternGroup = new SuperPattern(unexpandedPatternGroup);
                for (IPatternConstruct canonicalPattern : canonicalPatternGroup) {
                    canonicalPattern.accept(new AbstractGraphixQueryVisitor() {
                        @Override
                        public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
                            PathDescriptor canonicalPathDescriptor = ppe.getPathDescriptor();
                            if (canonicalPathDescriptor.getVariableExpr().equals(pathVariable)) {
                                for (IPatternConstruct patternConstruct : candidatePath) {
                                    switch (patternConstruct.getPatternType()) {
                                        case VERTEX_PATTERN:
                                            VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                                            vertexPatternExpr.setParentPathExpr(ppe);
                                            break;
                                        case EDGE_PATTERN:
                                            EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                                            edgePatternExpr.setParentPathExpr(ppe);
                                            break;
                                    }
                                }
                                canonicalPatternGroup.associateExpandedPath(ppe, candidatePath);
                            }
                            return super.visit(ppe, arg);
                        }
                    }, null);
                }
                outputPatternGroups.push(unifyVertexDescriptors(canonicalPatternGroup));
            }
        }
    }

    private SuperPattern unifyVertexDescriptors(SuperPattern patternGroup) throws CompilationException {
        Map<VariableExpr, Set<VertexDescriptor>> vertexDescriptorMap = new HashMap<>();
        for (IPatternConstruct patternConstruct : patternGroup) {
            patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
                    VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                    VariableExpr variableExpr = vertexDescriptor.getVariableExpr();
                    if (vpe.getParentVertexExpr() == null) {
                        vertexDescriptorMap.putIfAbsent(variableExpr, new HashSet<>());
                        vertexDescriptorMap.get(variableExpr).add(vertexDescriptor);

                    } else {
                        VertexDescriptor parentDescriptor = vpe.getParentVertexExpr().getVertexDescriptor();
                        VariableExpr parentExpr = parentDescriptor.getVariableExpr();
                        if (!vertexDescriptorMap.containsKey(parentExpr)) {
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                    "Encountered subquery vertex variable before source!");
                        }
                        vertexDescriptorMap.get(parentExpr).add(vertexDescriptor);
                    }
                    return super.visit(vpe, arg);
                }
            }, null);
        }

        // Sanity check: we should have at most one filter expression per vertex variable.
        for (Set<VertexDescriptor> descriptors : vertexDescriptorMap.values()) {
            Stream<Expression> filterExprStream = descriptors.stream().map(VertexDescriptor::getFilterExpr);
            if (filterExprStream.filter(Objects::nonNull).distinct().count() >= 2) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Encountered more than one filter expression for a single vertex!");
            }
        }

        // We do not want to modify the input pattern group.
        SuperPattern outputPatternGroup = new SuperPattern(patternGroup);
        for (IPatternConstruct patternConstruct : outputPatternGroup) {
            patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
                    VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                    VariableExpr variableExpr = vertexDescriptor.getVariableExpr();
                    if (vpe.getParentVertexExpr() == null) {
                        Set<ElementLabel> vertexLabels =
                                vertexDescriptorMap.get(variableExpr).stream().map(AbstractDescriptor::getLabels)
                                        .flatMap(Collection::stream).collect(Collectors.toSet());
                        vertexDescriptor.replaceLabels(vertexLabels);
                        Optional<Expression> filterExpr = vertexDescriptorMap.get(variableExpr).stream()
                                .map(VertexDescriptor::getFilterExpr).filter(Objects::nonNull).findAny();
                        if (filterExpr.isPresent() && vertexDescriptor.getFilterExpr() == null) {
                            vertexDescriptor.setFilterExpr(filterExpr.get());
                        }

                    } else {
                        VariableExpr parentExpr = vpe.getParentVertexExpr().getVertexDescriptor().getVariableExpr();
                        Set<ElementLabel> vertexLabels =
                                vertexDescriptorMap.get(parentExpr).stream().map(AbstractDescriptor::getLabels)
                                        .flatMap(Collection::stream).collect(Collectors.toSet());
                        vertexDescriptor.replaceLabels(vertexLabels);
                        Optional<Expression> filterExpr = vertexDescriptorMap.get(parentExpr).stream()
                                .map(VertexDescriptor::getFilterExpr).filter(Objects::nonNull).findAny();
                        if (filterExpr.isPresent() && vertexDescriptor.getFilterExpr() == null) {
                            // Note: this will be using the wrong variable at this point. We will remap later.
                            vertexDescriptor.setFilterExpr(filterExpr.get());
                        }
                    }
                    return super.visit(vpe, arg);
                }
            }, null);
        }
        return outputPatternGroup;
    }

    private List<SuperPattern> expandPatternGroups(SuperPattern patternGroup) throws CompilationException {
        // First pass: generate a pattern group that has unified vertex labels.
        SuperPattern unifiedLabelPatternGroup = unifyVertexDescriptors(patternGroup);

        // Second pass: collect all ambiguous graph elements.
        Set<IPatternConstruct> ambiguousConstructSet = new LinkedHashSet<>();
        for (IPatternConstruct patternConstruct : unifiedLabelPatternGroup) {
            switch (patternConstruct.getPatternType()) {
                case VERTEX_PATTERN:
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                    VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                    Set<ElementLabel> vertexLabels = vertexDescriptor.getLabels();
                    if (vertexLabels.isEmpty() || vertexLabels.stream().allMatch(ElementLabel::isNegated)) {
                        ambiguousConstructSet.add(vertexPatternExpr);
                    }
                    break;
                case EDGE_PATTERN:
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                    EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                    Set<ElementLabel> edgeLabels = edgeDescriptor.getLabels();
                    if (edgeLabels.size() != 1 || edgeLabels.stream().anyMatch(ElementLabel::isNegated)
                            || edgeDescriptor.getElementDirection() == ElementDirection.UNDIRECTED) {
                        ambiguousConstructSet.add(edgePatternExpr);
                    }
                    break;
                case PATH_PATTERN:
                    // We need to expand paths and identify the vertices required to realize our hops.
                    ambiguousConstructSet.add(patternConstruct);
                    break;
            }
        }
        if (ambiguousConstructSet.isEmpty()) {
            // What we return must always be mutable.
            return new ArrayList<>(List.of(unifiedLabelPatternGroup));
        }

        // Third pass: expand the ambiguous graph elements.
        Deque<SuperPattern> redPatternGroups = new ArrayDeque<>();
        Deque<SuperPattern> blackPatternGroups = new ArrayDeque<>();
        redPatternGroups.add(unifiedLabelPatternGroup);
        for (IPatternConstruct patternConstruct : ambiguousConstructSet) {
            Deque<SuperPattern> readPatternGroups, writePatternGroups;
            if (redPatternGroups.isEmpty()) {
                readPatternGroups = blackPatternGroups;
                writePatternGroups = redPatternGroups;

            } else {
                readPatternGroups = redPatternGroups;
                writePatternGroups = blackPatternGroups;
            }

            // Expand our vertex / edge patterns.
            switch (patternConstruct.getPatternType()) {
                case VERTEX_PATTERN:
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                    expandVertexPattern(vertexPatternExpr, readPatternGroups, writePatternGroups);
                    break;
                case EDGE_PATTERN:
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                    expandEdgePattern(edgePatternExpr, readPatternGroups, writePatternGroups);
                    break;
                case PATH_PATTERN:
                    PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                    expandPathPattern(pathPatternExpr, readPatternGroups, writePatternGroups);
                    break;
            }
        }
        return new ArrayList<>(redPatternGroups.isEmpty() ? blackPatternGroups : redPatternGroups);
    }

    private List<SuperPattern> pruneInvalidGroups(List<SuperPattern> sourceGroups) {
        ListIterator<SuperPattern> sourceGroupIterator = sourceGroups.listIterator();
        while (sourceGroupIterator.hasNext()) {
            SuperPattern workingPatternGroup = sourceGroupIterator.next();
            for (IPatternConstruct patternConstruct : workingPatternGroup) {
                if (patternConstruct.getPatternType() == IPatternConstruct.PatternType.EDGE_PATTERN) {
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                    EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();

                    // Prune any groups with bad edges.
                    VertexDescriptor leftDescriptor = edgePatternExpr.getLeftVertex().getVertexDescriptor();
                    VertexDescriptor rightDescriptor = edgePatternExpr.getRightVertex().getVertexDescriptor();
                    ElementLabel leftLabel = leftDescriptor.getLabels().iterator().next();
                    ElementLabel rightLabel = rightDescriptor.getLabels().iterator().next();
                    ElementLabel edgeLabel = edgeDescriptor.getLabels().iterator().next();
                    ElementDirection edgeDirection = edgeDescriptor.getElementDirection();
                    SchemaQuery edgeQuery = new SchemaQuery(leftLabel, edgeLabel, rightLabel, edgeDirection);
                    if (!schemaKnowledgeTable.answerQuery(edgeQuery)) {
                        sourceGroupIterator.remove();
                        break;
                    }
                } else if (patternConstruct.getPatternType() == IPatternConstruct.PatternType.PATH_PATTERN) {
                    PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;

                    // Iterate through the decomposition of our path.
                    QueryPatternExpr decomposition = workingPatternGroup.getExpandedPath(pathPatternExpr);
                    boolean isValidDecomposition = StreamSupport.stream(decomposition.spliterator(), false)
                            .filter(i -> i.getPatternType() == IPatternConstruct.PatternType.EDGE_PATTERN)
                            .map(e -> (EdgePatternExpr) e).allMatch(e -> {
                                VertexDescriptor leftDescriptor = e.getLeftVertex().getVertexDescriptor();
                                VertexDescriptor rightDescriptor = e.getRightVertex().getVertexDescriptor();
                                ElementLabel leftLabel = leftDescriptor.getLabels().iterator().next();
                                ElementLabel rightLabel = rightDescriptor.getLabels().iterator().next();
                                ElementLabel edgeLabel = e.getEdgeDescriptor().getLabels().iterator().next();
                                ElementDirection edgeDirection = e.getEdgeDescriptor().getElementDirection();
                                SchemaQuery q = new SchemaQuery(leftLabel, edgeLabel, rightLabel, edgeDirection);
                                return schemaKnowledgeTable.answerQuery(q);
                            });
                    if (!isValidDecomposition) {
                        sourceGroupIterator.remove();
                        break;
                    }
                }
            }
        }
        return sourceGroups;
    }

    @Override
    public void resolve(SuperPattern superPattern) throws CompilationException {
        Map<VariableExpr, Set<ElementLabel>> resolvedElementLabels = new LinkedHashMap<>();
        Map<VariableExpr, Expression> resolvedFilterExpressions = new LinkedHashMap<>();
        Map<VariableExpr, Set<ElementDirection>> resolvedElementDirections = new LinkedHashMap<>();
        Map<VariableExpr, Set<EdgePatternExpr>> resolvedPathDecompositions = new LinkedHashMap<>();

        // Populate our variable maps. This will help us find offending elements (if any exist).
        for (IPatternConstruct patternConstruct : superPattern) {
            switch (patternConstruct.getPatternType()) {
                case VERTEX_PATTERN:
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                    VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                    VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
                    if (vertexPatternExpr.getParentVertexExpr() == null) {
                        resolvedElementLabels.putIfAbsent(vertexVariable, new HashSet<>());
                    }
                    break;
                case EDGE_PATTERN:
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                    EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                    VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                    resolvedElementLabels.put(edgeVariable, new HashSet<>());
                    resolvedElementDirections.put(edgeVariable, new HashSet<>());
                    break;
                case PATH_PATTERN:
                    PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                    PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
                    VariableExpr pathVariable = pathDescriptor.getVariableExpr();
                    resolvedElementLabels.put(pathVariable, new HashSet<>());
                    resolvedElementDirections.put(pathVariable, new HashSet<>());
                    resolvedPathDecompositions.put(pathVariable, new HashSet<>());
                    break;
            }
        }

        // Expand the given pattern group and then prune the invalid pattern groups.
        List<SuperPattern> validPatternGroups = pruneInvalidGroups(expandPatternGroups(superPattern));
        for (SuperPattern validPatternGroup : validPatternGroups) {
            for (IPatternConstruct patternConstruct : validPatternGroup) {
                switch (patternConstruct.getPatternType()) {
                    case VERTEX_PATTERN:
                        VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                        VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                        Expression filterExpr = vertexDescriptor.getFilterExpr();
                        VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
                        if (vertexPatternExpr.getParentVertexExpr() == null) {
                            resolvedElementLabels.get(vertexVariable).addAll(vertexDescriptor.getLabels());
                        }
                        if (!resolvedFilterExpressions.containsKey(vertexVariable) && filterExpr != null) {
                            resolvedFilterExpressions.put(vertexVariable, filterExpr);
                        }
                        break;
                    case EDGE_PATTERN:
                        EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                        VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                        resolvedElementLabels.get(edgeVariable).addAll(edgeDescriptor.getLabels());
                        resolvedElementDirections.get(edgeVariable).add(edgeDescriptor.getElementDirection());
                        break;
                    case PATH_PATTERN:
                        PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                        PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
                        VariableExpr pathVariable = pathDescriptor.getVariableExpr();
                        resolvedElementLabels.get(pathVariable).addAll(pathDescriptor.getLabels());
                        resolvedElementDirections.get(pathVariable).add(pathDescriptor.getElementDirection());
                        StreamSupport.stream(validPatternGroup.getExpandedPath(pathPatternExpr).spliterator(), false)
                                .filter(e -> e.getPatternType() == IPatternConstruct.PatternType.EDGE_PATTERN)
                                .map(e -> (EdgePatternExpr) e).distinct()
                                .forEach(e -> resolvedPathDecompositions.get(pathVariable).add(e));
                        break;
                }
            }
        }

        // Propagate our answers to the original expressions.
        for (Map.Entry<VariableExpr, Expression> entry : resolvedFilterExpressions.entrySet()) {
            Expression resolvedExpr = entry.getValue();
            VariableExpr variableExpr = entry.getKey();
            for (IPatternConstruct patternConstruct : superPattern) {
                patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                    final VariableRemapCloneVisitor remapCloneVisitor = new VariableRemapCloneVisitor(rewritingContext);

                    @Override
                    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
                        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                        boolean hasFilterExpr = vertexDescriptor.getFilterExpr() != null;
                        if (vpe.getParentVertexExpr() != null) {
                            VertexDescriptor parentDescriptor = vpe.getParentVertexExpr().getVertexDescriptor();
                            if (Objects.equals(parentDescriptor.getVariableExpr(), variableExpr) && !hasFilterExpr) {
                                remapCloneVisitor.addSubstitution(variableExpr, vertexDescriptor.getVariableExpr());
                                vertexDescriptor.setFilterExpr((Expression) remapCloneVisitor.substitute(resolvedExpr));
                            }

                        } else if (vertexDescriptor.getVariableExpr().equals(variableExpr) && !hasFilterExpr) {
                            vertexDescriptor.setFilterExpr((Expression) remapCloneVisitor.substitute(resolvedExpr));
                        }
                        return super.visit(vpe, arg);
                    }
                }, null);
            }
        }
        for (Map.Entry<VariableExpr, Set<EdgePatternExpr>> entry : resolvedPathDecompositions.entrySet()) {
            Set<EdgePatternExpr> resolvedDecompositions = entry.getValue();
            VariableExpr variableExpr = entry.getKey();
            if (resolvedDecompositions.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, variableExpr.getSourceLocation(),
                        "Encountered graph element that does not conform the queried graph schema!");
            }
            for (IPatternConstruct patternConstruct : superPattern) {
                patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                    @Override
                    public Expression visit(PathPatternExpr ppe, ILangExpression arg) {
                        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
                        VariableExpr pathVariable = pathDescriptor.getVariableExpr();

                        // Do not add duplicate decompositions.
                        Set<SchemaQuery> addedDecompositionSet = new LinkedHashSet<>();
                        for (EdgePatternExpr decomposition : resolvedPathDecompositions.get(pathVariable)) {
                            EdgeDescriptor decompositionDescriptor = decomposition.getEdgeDescriptor();
                            SchemaQuery addedKey = new SchemaQuery(
                                    decomposition.getLeftVertex().getVertexDescriptor().getLabels().iterator().next(),
                                    decompositionDescriptor.getLabels().iterator().next(),
                                    decomposition.getRightVertex().getVertexDescriptor().getLabels().iterator().next(),
                                    decompositionDescriptor.getElementDirection());
                            if (!addedDecompositionSet.add(addedKey)) {
                                continue;
                            }

                            // Our decomposition (and associated vertices) should refer to this path.
                            decomposition.setParentPathExpr(ppe);
                            decomposition.getLeftVertex().setParentPathExpr(ppe);
                            decomposition.getRightVertex().setParentPathExpr(ppe);

                            // Obfuscate our variable names when entered as a decomposition to avoid confusion.
                            VariableExpr newLeftVariable = new VariableExpr(rewritingContext.newVariable());
                            VariableExpr newRightVariable = new VariableExpr(rewritingContext.newVariable());
                            VariableExpr newEdgeVariable = new VariableExpr(rewritingContext.newVariable());
                            decomposition.getLeftVertex().getVertexDescriptor().setVariableExpr(newLeftVariable);
                            decomposition.getRightVertex().getVertexDescriptor().setVariableExpr(newRightVariable);
                            decomposition.getEdgeDescriptor().setVariableExpr(newEdgeVariable);
                            ppe.addDecomposition(decomposition);
                        }
                        return ppe;
                    }
                }, null);
            }
        }
        for (Map.Entry<VariableExpr, Set<ElementLabel>> entry : resolvedElementLabels.entrySet()) {
            Set<ElementLabel> resolvedLabels = entry.getValue();
            VariableExpr variableExpr = entry.getKey();
            if (resolvedLabels.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, variableExpr.getSourceLocation(),
                        "Encountered graph element that does not conform the queried graph schema!");
            }
            for (IPatternConstruct patternConstruct : superPattern) {
                patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                    @Override
                    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) {
                        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                        if (vpe.getParentVertexExpr() != null) {
                            VertexDescriptor parentDescriptor = vpe.getParentVertexExpr().getVertexDescriptor();
                            if (!parentDescriptor.getVariableExpr().equals(variableExpr)) {
                                return vpe;
                            }
                        } else if (!vertexDescriptor.getVariableExpr().equals(variableExpr)) {
                            return vpe;
                        }
                        for (ElementLabel resolvedLabel : resolvedLabels) {
                            if (!vertexDescriptor.getLabels().contains(resolvedLabel) && LOGGER.isDebugEnabled()) {
                                SourceLocation sourceLocation = vpe.getSourceLocation();
                                String vertexLine = String.format("line %s", sourceLocation.getLine());
                                String vertexColumn = String.format("column %s", sourceLocation.getColumn());
                                String vertexLocation = String.format("(%s, %s)", vertexLine, vertexColumn);
                                String logString = "Label {} has been resolved for vertex {} {}.";
                                LOGGER.debug(logString, resolvedLabel, variableExpr, vertexLocation);
                            }
                        }
                        vertexDescriptor.replaceLabels(resolvedLabels);
                        return vpe;
                    }

                    @Override
                    public Expression visit(EdgePatternExpr epe, ILangExpression arg) {
                        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
                        if (!edgeDescriptor.getVariableExpr().equals(variableExpr)) {
                            return epe;
                        }
                        for (ElementLabel resolvedLabel : resolvedLabels) {
                            if (edgeDescriptor.getLabels().contains(resolvedLabel) && LOGGER.isDebugEnabled()) {
                                SourceLocation sourceLocation = epe.getSourceLocation();
                                String edgeLine = String.format("line %s", sourceLocation.getLine());
                                String edgeColumn = String.format("column %s", sourceLocation.getColumn());
                                String edgeLocation = String.format("(%s, %s)", edgeLine, edgeColumn);
                                String logString = "Label {} has been resolved for edge {} {}.";
                                LOGGER.debug(logString, resolvedLabel, variableExpr, edgeLocation);
                            }
                        }
                        edgeDescriptor.replaceLabels(resolvedLabels);
                        return epe;
                    }

                    @Override
                    public Expression visit(PathPatternExpr ppe, ILangExpression arg) {
                        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
                        if (!pathDescriptor.getVariableExpr().equals(variableExpr)) {
                            return ppe;
                        }
                        for (ElementLabel resolvedLabel : resolvedLabels) {
                            if (pathDescriptor.getLabels().contains(resolvedLabel) && LOGGER.isDebugEnabled()) {
                                SourceLocation sourceLocation = ppe.getSourceLocation();
                                String pathLine = String.format("line %s", sourceLocation.getLine());
                                String pathColumn = String.format("column %s", sourceLocation.getColumn());
                                String pathLocation = String.format("(%s, %s)", pathLine, pathColumn);
                                String logString = "Label {} has been resolved for path {} {}.";
                                LOGGER.debug(logString, resolvedLabel, variableExpr, pathLocation);
                            }
                        }
                        pathDescriptor.replaceLabels(resolvedLabels);
                        return ppe;
                    }
                }, null);
            }
        }
        for (Map.Entry<VariableExpr, Set<ElementDirection>> entry : resolvedElementDirections.entrySet()) {
            Set<ElementDirection> resolvedDirections = entry.getValue();
            VariableExpr variableExpr = entry.getKey();
            if (resolvedDirections.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, variableExpr.getSourceLocation(),
                        "Encountered graph element that does not conform the queried graph schema!");
            }
            for (IPatternConstruct patternConstruct : superPattern) {
                patternConstruct.accept(new AbstractGraphixQueryVisitor() {
                    @Override
                    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
                        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
                        if (!edgeDescriptor.getVariableExpr().equals(variableExpr)) {
                            return epe;
                        }
                        if (resolvedDirections.size() == 1) {
                            ElementDirection resolvedDirection = resolvedDirections.iterator().next();
                            if (edgeDescriptor.setElementDirection(resolvedDirection) && LOGGER.isDebugEnabled()) {
                                SourceLocation sourceLocation = epe.getSourceLocation();
                                String edgeLine = String.format("line %s", sourceLocation.getLine());
                                String edgeColumn = String.format("column %s", sourceLocation.getColumn());
                                String edgeLocation = String.format("(%s, %s)", edgeLine, edgeColumn);
                                String logString = "Direction {} has been resolved for edge {} {}.";
                                LOGGER.debug(logString, resolvedDirection, variableExpr, edgeLocation);
                            }
                        }
                        return epe;
                    }

                    @Override
                    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
                        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
                        if (!pathDescriptor.getVariableExpr().equals(variableExpr)) {
                            return ppe;
                        }
                        if (resolvedDirections.size() == 1) {
                            ElementDirection resolvedDirection = resolvedDirections.iterator().next();
                            if (pathDescriptor.setElementDirection(resolvedDirection) && LOGGER.isDebugEnabled()) {
                                SourceLocation sourceLocation = ppe.getSourceLocation();
                                String pathLine = String.format("line %s", sourceLocation.getLine());
                                String pathColumn = String.format("column %s", sourceLocation.getColumn());
                                String pathLocation = String.format("(%s, %s)", pathLine, pathColumn);
                                String logString = "Direction {} has been resolved for path {} {}.";
                                LOGGER.debug(logString, resolvedDirection, variableExpr, pathLocation);
                            }
                        }
                        return ppe;
                    }
                }, null);
            }
        }
    }
}
