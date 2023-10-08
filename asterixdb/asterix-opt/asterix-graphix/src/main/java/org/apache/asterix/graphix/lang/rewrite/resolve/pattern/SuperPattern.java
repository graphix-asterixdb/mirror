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
package org.apache.asterix.graphix.lang.rewrite.resolve.pattern;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.commons.collections4.iterators.IteratorChain;

/**
 * A container for a collection of {@link IPatternConstruct} instances that belong to the same graph. In contrast to
 * {@link org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr}, these instances are not necessarily
 * adjacent.
 */
public class SuperPattern implements Iterable<IPatternConstruct> {
    private final List<VertexPatternExpr> vertexPatternList = new ArrayList<>();
    private final List<EdgePatternExpr> edgePatternList = new ArrayList<>();
    private final List<PathPatternExpr> pathPatternList = new ArrayList<>();
    private final Map<PathPatternExpr, QueryPatternExpr> decompositionMap = new LinkedHashMap<>();

    public SuperPattern() {
    }

    public SuperPattern(SuperPattern sourceSuperPattern) throws CompilationException {
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        for (IPatternConstruct patternConstruct : sourceSuperPattern) {
            switch (patternConstruct.getPatternType()) {
                case VERTEX_PATTERN:
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                    vertexPatternList.add(deepCopyVisitor.visit(vertexPatternExpr, null));
                    break;
                case EDGE_PATTERN:
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                    edgePatternList.add(deepCopyVisitor.visit(edgePatternExpr, null));
                    break;
                case PATH_PATTERN:
                    PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                    PathPatternExpr copyPatternExpr = deepCopyVisitor.visit(pathPatternExpr, null);
                    pathPatternList.add(copyPatternExpr);
                    if (sourceSuperPattern.decompositionMap.containsKey(pathPatternExpr)) {
                        QueryPatternExpr explodedPath = sourceSuperPattern.decompositionMap.get(pathPatternExpr);
                        QueryPatternExpr copyExplodedPath = deepCopyVisitor.visit(explodedPath, null);
                        for (IPatternConstruct innerConstruct : copyExplodedPath) {
                            switch (innerConstruct.getPatternType()) {
                                case VERTEX_PATTERN:
                                    VertexPatternExpr innerVertexPatternExpr = (VertexPatternExpr) innerConstruct;
                                    innerVertexPatternExpr.setParentPathExpr(copyPatternExpr);
                                    break;
                                case EDGE_PATTERN:
                                    EdgePatternExpr innerEdgePatternExpr = (EdgePatternExpr) innerConstruct;
                                    innerEdgePatternExpr.setParentPathExpr(copyPatternExpr);
                                    break;
                            }
                        }
                        decompositionMap.put(copyPatternExpr, copyExplodedPath);
                    }
                    break;
            }
        }
    }

    public void addPatternConstruct(IPatternConstruct patternConstruct) {
        switch (patternConstruct.getPatternType()) {
            case VERTEX_PATTERN:
                vertexPatternList.add((VertexPatternExpr) patternConstruct);
                break;
            case EDGE_PATTERN:
                edgePatternList.add((EdgePatternExpr) patternConstruct);
                break;
            case PATH_PATTERN:
                pathPatternList.add((PathPatternExpr) patternConstruct);
                break;
        }
    }

    public void associateExpandedPath(PathPatternExpr pathPatternExpr, QueryPatternExpr queryPatternExpr) {
        if (!pathPatternList.contains(pathPatternExpr)) {
            throw new IllegalStateException("Path not found!");
        }
        decompositionMap.put(pathPatternExpr, queryPatternExpr);
    }

    public QueryPatternExpr getExpandedPath(PathPatternExpr pathPatternExpr) {
        return decompositionMap.get(pathPatternExpr);
    }

    @Override
    public Iterator<IPatternConstruct> iterator() {
        IteratorChain<IPatternConstruct> iteratorChain = new IteratorChain<>();
        iteratorChain.addIterator(vertexPatternList.stream().sorted((v1, v2) -> {
            boolean doesV1HaveParent = v1.getParentVertexExpr() != null;
            boolean doesV2HaveParent = v2.getParentVertexExpr() != null;

            // Parent vertices should come before children.
            return (doesV1HaveParent && !doesV2HaveParent) ? 1 : ((!doesV1HaveParent && doesV2HaveParent) ? -1 : 0);
        }).iterator());
        iteratorChain.addIterator(edgePatternList.iterator());
        iteratorChain.addIterator(pathPatternList.iterator());
        return iteratorChain;
    }

    public boolean contains(SuperPattern superPattern) {
        Set<VertexPatternExpr> vertexPatternSet = new HashSet<>(this.vertexPatternList);
        Set<EdgePatternExpr> edgePatternSet = new HashSet<>(this.edgePatternList);
        Set<PathPatternExpr> pathPatternSet = new HashSet<>(this.pathPatternList);
        return vertexPatternSet.containsAll(superPattern.vertexPatternList)
                && edgePatternSet.containsAll(superPattern.edgePatternList)
                && pathPatternSet.containsAll(superPattern.pathPatternList)
                && this.decompositionMap.entrySet().containsAll(superPattern.decompositionMap.entrySet());
    }

    public int size() {
        return vertexPatternList.size() + edgePatternList.size() + pathPatternList.size() + decompositionMap.size();
    }
}
