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
package org.apache.asterix.graphix.lang.rewrite.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.LeftLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.RightLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.paukov.combinatorics3.Generator;

public final class PatternExpansionUtil {
    public static Set<ElementLabel> expandElementLabels(Set<ElementLabel> labels, Set<ElementLabel> schemaLabels)
            throws CompilationException {
        Set<ElementLabel> expandedLabelSet;
        if (!labels.isEmpty()) {
            // Either all labels should be negated, or none of them should be negated.
            boolean hasNegatedLabels = labels.stream().anyMatch(ElementLabel::isNegated);
            boolean hasConfirmedLabels = labels.stream().anyMatch(e -> !e.isNegated());
            if (hasNegatedLabels && hasConfirmedLabels) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, "Bad regular path expression given!");
            }

            // If we are working with negated element labels, then return the difference between our schema and this.
            Stream<ElementLabel> expandedLabelStream =
                    !hasNegatedLabels ? labels.stream() : schemaLabels.stream().filter(e -> !labels.contains(e));
            expandedLabelSet = expandedLabelStream.collect(Collectors.toCollection(HashSet::new));

        } else {
            // If our set is empty, then return all the element labels associated with our schema.
            expandedLabelSet = new HashSet<>(schemaLabels);
        }
        return expandedLabelSet;
    }

    public static Set<ElementDirection> expandElementDirection(ElementDirection elementDirection) {
        Set<ElementDirection> elementDirectionSet = new HashSet<>();
        if (elementDirection != ElementDirection.RIGHT_TO_LEFT) {
            elementDirectionSet.add(ElementDirection.LEFT_TO_RIGHT);
        }
        if (elementDirection != ElementDirection.LEFT_TO_RIGHT) {
            elementDirectionSet.add(ElementDirection.RIGHT_TO_LEFT);
        }
        return elementDirectionSet;
    }

    public static List<QueryPatternExpr> explodePathPattern(PathPatternExpr pathPatternExpr, int pathLength,
            Set<ElementLabel> edgeLabels, Set<ElementLabel> vertexLabels, Set<ElementDirection> edgeDirections,
            GraphixRewritingContext rewritingContext) throws CompilationException {
        PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
        VertexPatternExpr leftVertex = pathPatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = pathPatternExpr.getRightVertex();
        VariableExpr pathVariableExpr = pathDescriptor.getVariableExpr();
        VariableExpr leftVariableExpr = leftVertex.getVertexDescriptor().getVariableExpr();
        VariableExpr rightVariableExpr = rightVertex.getVertexDescriptor().getVariableExpr();

        // Generate all possible edge label K-permutations w/ repetitions.
        List<List<ElementLabel>> edgeLabelPermutations = new ArrayList<>();
        Generator.combination(edgeLabels).multi(pathLength).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(edgeLabelPermutations::add));

        // Generate all possible direction K-permutations w/ repetitions.
        List<List<ElementDirection>> directionPermutations = new ArrayList<>();
        Generator.combination(edgeDirections).multi(pathLength).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(directionPermutations::add));

        // Special case: if we have a path of length one, we do not need to permute our vertices.
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        if (pathLength == 1) {
            List<QueryPatternExpr> expandedPathPatterns = new ArrayList<>();
            for (List<ElementLabel> edgeLabelPermutation : edgeLabelPermutations) {
                ElementLabel edgeLabel = edgeLabelPermutation.get(0);
                for (List<ElementDirection> directionPermutation : directionPermutations) {

                    // Copy our vertices...
                    VertexPatternExpr leftVertexClone = deepCopyVisitor.visit(leftVertex, null);
                    VertexPatternExpr rightVertexClone = deepCopyVisitor.visit(rightVertex, null);
                    VariableExpr leftVariableCopy = rewritingContext.getGraphixVariableCopy(leftVariableExpr);
                    VariableExpr rightVariableCopy = rewritingContext.getGraphixVariableCopy(rightVariableExpr);
                    VertexDescriptor leftCloneDescriptor = leftVertexClone.getVertexDescriptor();
                    VertexDescriptor rightCloneDescriptor = rightVertexClone.getVertexDescriptor();
                    leftCloneDescriptor.setVariableExpr(leftVariableCopy);
                    rightCloneDescriptor.setVariableExpr(rightVariableCopy);
                    leftVertexClone.setParentPathExpr(pathPatternExpr);
                    rightVertexClone.setParentPathExpr(pathPatternExpr);
                    leftCloneDescriptor.removeFilterExpr();
                    rightCloneDescriptor.removeFilterExpr();

                    // ...and our edges.
                    EdgeDescriptor edgeDescriptor = new EdgeDescriptor(directionPermutation.get(0),
                            rewritingContext.getGraphixVariableCopy(pathVariableExpr), Set.of(edgeLabel), null);
                    QueryPatternExpr.Builder patternBuilder = new QueryPatternExpr.Builder(leftVertexClone);
                    EdgePatternExpr edgePatternExpr =
                            new EdgePatternExpr(leftVertexClone, rightVertexClone, edgeDescriptor);
                    if (pathPatternExpr.hasHints()) {
                        List<IExpressionAnnotation> linkOnlyHints = pathPatternExpr.getHints().stream().filter(
                                h -> h instanceof LeftLinkJoinAnnotation || h instanceof RightLinkJoinAnnotation)
                                .collect(Collectors.toList());
                        edgePatternExpr.addHints(linkOnlyHints);
                    }
                    edgePatternExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
                    edgePatternExpr.setParentPathExpr(pathPatternExpr);
                    patternBuilder.addEdge(edgePatternExpr);
                    expandedPathPatterns.add(patternBuilder.build());
                }
            }
            return expandedPathPatterns;
        }

        // Otherwise... generate all possible (K-1)-permutations w/ repetitions.
        List<List<ElementLabel>> vertexLabelPermutations = new ArrayList<>();
        Generator.combination(vertexLabels).multi(pathLength - 1).stream()
                .forEach(c -> Generator.permutation(c).simple().forEach(vertexLabelPermutations::add));

        // ... and perform a cartesian product of all three sets above.
        List<QueryPatternExpr> expandedPathPatterns = new ArrayList<>();
        for (List<ElementLabel> edgeLabelPermutation : edgeLabelPermutations) {
            for (List<ElementDirection> directionPermutation : directionPermutations) {
                for (List<ElementLabel> vertexLabelPermutation : vertexLabelPermutations) {
                    Iterator<ElementLabel> edgeLabelIterator = edgeLabelPermutation.iterator();
                    Iterator<ElementDirection> directionIterator = directionPermutation.iterator();
                    Iterator<ElementLabel> vertexLabelIterator = vertexLabelPermutation.iterator();

                    // Build one path.
                    VertexPatternExpr leftVertexClone = deepCopyVisitor.visit(leftVertex, null);
                    VariableExpr leftCopyVariable = rewritingContext.getGraphixVariableCopy(leftVariableExpr);
                    leftVertexClone.getVertexDescriptor().setVariableExpr(leftCopyVariable);
                    leftVertexClone.getVertexDescriptor().removeFilterExpr();
                    QueryPatternExpr.Builder queryPatternBuilder = new QueryPatternExpr.Builder(leftVertexClone);
                    VertexPatternExpr previousRightVertex = null;
                    for (int i = 0; edgeLabelIterator.hasNext(); i++) {
                        Set<ElementLabel> edgeLabelSet = Set.of(edgeLabelIterator.next());
                        ElementDirection edgeDirection = directionIterator.next();

                        // Determine our left vertex.
                        VertexPatternExpr workingLeftVertex = (i == 0) ? leftVertexClone : previousRightVertex;

                        // Determine our right vertex.
                        VertexPatternExpr workingRightVertex;
                        if (!edgeLabelIterator.hasNext()) {
                            workingRightVertex = deepCopyVisitor.visit(rightVertex, null);
                            VariableExpr rightCopyVariable = rewritingContext.getGraphixVariableCopy(rightVariableExpr);
                            workingRightVertex.getVertexDescriptor().setVariableExpr(rightCopyVariable);
                            workingRightVertex.getVertexDescriptor().removeFilterExpr();

                        } else {
                            VariableExpr vertexVariable = rewritingContext.getGraphixVariableCopy(pathVariableExpr);
                            VertexDescriptor vertexDescriptor =
                                    new VertexDescriptor(vertexVariable, Set.of(vertexLabelIterator.next()), null);
                            workingRightVertex = new VertexPatternExpr(vertexDescriptor);
                            workingRightVertex.setSourceLocation(pathPatternExpr.getSourceLocation());
                            workingRightVertex.setParentPathExpr(pathPatternExpr);
                            workingRightVertex.getVertexDescriptor().removeFilterExpr();
                        }
                        previousRightVertex = workingRightVertex;

                        // And build the edge to add to our path.
                        VariableExpr edgeVariable = rewritingContext.getGraphixVariableCopy(pathVariableExpr);
                        EdgePatternExpr edgePatternExpr = new EdgePatternExpr(workingLeftVertex, workingRightVertex,
                                new EdgeDescriptor(edgeDirection, edgeVariable, edgeLabelSet, null));
                        edgePatternExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
                        edgePatternExpr.setParentPathExpr(pathPatternExpr);
                        queryPatternBuilder.addEdge(edgePatternExpr);
                    }
                    expandedPathPatterns.add(queryPatternBuilder.build());
                }
            }
        }
        return expandedPathPatterns;
    }

    private PatternExpansionUtil() {
    }
}
