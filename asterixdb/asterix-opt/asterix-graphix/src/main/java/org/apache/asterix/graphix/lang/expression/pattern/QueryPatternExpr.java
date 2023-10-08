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
package org.apache.asterix.graphix.lang.expression.pattern;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query pattern is composed of the following:
 * <ul>
 *   <li>A list of {@link VertexPatternExpr} instances.</li>
 *   <li>A list of {@link EdgePatternExpr} instances.</li>
 *   <li>A list of {@link PathPatternExpr} instances.</li>
 * </ul>
 * All {@link IPatternConstruct} instances associated with a query pattern are connected, as should be iterated over
 * in the order of their adjacency.
 */
public class QueryPatternExpr extends AbstractExpression implements Iterable<IPatternConstruct> {
    protected final List<IPatternConstruct> patternExpressions;

    public QueryPatternExpr(List<IPatternConstruct> patternExpressions) {
        this.patternExpressions = patternExpressions;

        // Verify that our query pattern is connected.
        Iterator<IPatternConstruct> patternIterator = patternExpressions.iterator();
        VertexPatternExpr workingLeftVertex = (VertexPatternExpr) patternIterator.next();
        VertexPatternExpr workingRightVertex = null;
        while (patternIterator.hasNext()) {
            IPatternConstruct workingPattern = patternIterator.next();
            switch (workingPattern.getPatternType()) {
                case VERTEX_PATTERN:
                    VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) workingPattern;
                    if (!Objects.equals(workingRightVertex, vertexPatternExpr)) {
                        throw new IllegalArgumentException("Disconnected QueryPatternExpr encountered!");
                    }
                    workingLeftVertex = vertexPatternExpr;
                    break;
                case EDGE_PATTERN:
                    EdgePatternExpr edgePatternExpr = (EdgePatternExpr) workingPattern;
                    if (!Objects.equals(edgePatternExpr.getLeftVertex(), workingLeftVertex)) {
                        throw new IllegalArgumentException("Disconnected QueryPatternExpr encountered!");
                    }
                    workingRightVertex = edgePatternExpr.getRightVertex();
                    break;
                case PATH_PATTERN:
                    PathPatternExpr pathPatternExpr = (PathPatternExpr) workingPattern;
                    if (!Objects.equals(pathPatternExpr.getLeftVertex(), workingLeftVertex)) {
                        throw new IllegalArgumentException("Disconnected QueryPatternExpr encountered!");
                    }
                    workingRightVertex = pathPatternExpr.getRightVertex();
                    break;
            }
        }
    }

    public static class Builder {
        private final List<IPatternConstruct> patternExpressions = new ArrayList<>();

        public Builder(VertexPatternExpr leftVertexExpr) {
            patternExpressions.add(leftVertexExpr);
        }

        public void addEdge(EdgePatternExpr edgePatternExpr) throws CompilationException {
            VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
            VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
            if (!patternExpressions.contains(leftVertex)) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Attempting to build disconnected pattern!");
            }
            patternExpressions.add(edgePatternExpr);
            patternExpressions.add(rightVertex);
        }

        public void addPath(PathPatternExpr pathPatternExpr) throws CompilationException {
            VertexPatternExpr leftVertex = pathPatternExpr.getLeftVertex();
            VertexPatternExpr rightVertex = pathPatternExpr.getRightVertex();
            if (!patternExpressions.contains(leftVertex)) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Attempting to build disconnected pattern!");
            }
            patternExpressions.add(pathPatternExpr);
            patternExpressions.add(rightVertex);
        }

        public QueryPatternExpr build() {
            return new QueryPatternExpr(patternExpressions);
        }
    }

    @Override
    public Iterator<IPatternConstruct> iterator() {
        return patternExpressions.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (IPatternConstruct patternConstruct : this) {
            sb.append(patternConstruct.toString());
        }
        return sb.toString();
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);

    }
}
