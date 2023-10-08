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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * A query path pattern instance.
 * <ul>
 *   <li>A query path, in addition the fields in {@link PathDescriptor}, maintains a set of the following (indirectly,
 *   via its corresponding {@link QueryPatternExpr}):
 *   <ol>
 *     <li>{@link GraphElementDeclaration}s that contain the body from the corresponding
 *     {@link org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor} <b>and</b>
 *     {@link org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor}.</li>
 *     <li>{@link Edge} and {@link Vertex} that contains the key fields associated with the aforementioned
 *     mappings.</li>
 *   </ol></li>
 *   <li>A query path maintains a pair of terminal {@link VertexPatternExpr}s, used to describe the endpoint vertices
 *   of our path.</li>
 *   <li>A query path possesses a set of {@link EdgePatternExpr} instances (referred to as decompositions), used to
 *   describe how we navigate between our terminal {@link VertexPatternExpr} instances.</li>
 * </ul>
 *
 * @see PathDescriptor
 */
public class PathPatternExpr extends AbstractExpression implements IPatternConstruct {
    private final PathDescriptor pathDescriptor;
    private final VertexPatternExpr leftPatternExpr;
    private final VertexPatternExpr rightPatternExpr;

    // The following are set as we walk / rewrite our AST.
    private final List<EdgePatternExpr> decompositionExprs;
    private boolean arePropertiesUsed = false;

    public PathPatternExpr(VertexPatternExpr leftVertex, VertexPatternExpr rightVertex, PathDescriptor pathDescriptor) {
        this.decompositionExprs = new ArrayList<>();
        this.pathDescriptor = pathDescriptor;
        this.leftPatternExpr = leftVertex;
        this.rightPatternExpr = rightVertex;
    }

    public VertexPatternExpr getLeftVertex() {
        return leftPatternExpr;
    }

    public VertexPatternExpr getRightVertex() {
        return rightPatternExpr;
    }

    public PathDescriptor getPathDescriptor() {
        return pathDescriptor;
    }

    public List<EdgePatternExpr> getDecompositions() {
        return Collections.unmodifiableList(decompositionExprs);
    }

    public boolean arePropertiesUsed() {
        return arePropertiesUsed;
    }

    public void addDecomposition(EdgePatternExpr decompositionExpr) {
        if (!Objects.equals(decompositionExpr.getLeftVertex().getParentPathExpr(), this)) {
            throw new IllegalArgumentException("Decomposition vertex found that does not refer to this!");
        }
        if (!Objects.equals(decompositionExpr.getRightVertex().getParentPathExpr(), this)) {
            throw new IllegalArgumentException("Decomposition vertex found that does not refer to this!");
        }
        if (!Objects.equals(decompositionExpr.getParentPathExpr(), this)) {
            throw new IllegalArgumentException("Decomposition edge found that does not refer to this!");
        }

        // Ignoring our variable names, only add this decomposition if we have seen it before.
        VertexDescriptor newLeftDescriptor = decompositionExpr.getLeftVertex().getVertexDescriptor();
        VertexDescriptor newRightDescriptor = decompositionExpr.getRightVertex().getVertexDescriptor();
        EdgeDescriptor newEdgeDescriptor = decompositionExpr.getEdgeDescriptor();
        boolean hasDuplicate = decompositionExprs.stream().anyMatch(e -> {
            // First, our vertex labels...
            VertexDescriptor existingLeftDescriptor = e.getLeftVertex().getVertexDescriptor();
            VertexDescriptor existingRightDescriptor = e.getRightVertex().getVertexDescriptor();
            boolean doLeftLabelsMatch = newLeftDescriptor.getLabels().containsAll(existingLeftDescriptor.getLabels())
                    && existingLeftDescriptor.getLabels().containsAll(newLeftDescriptor.getLabels());
            boolean doRightLabelsMatch = newRightDescriptor.getLabels().containsAll(existingRightDescriptor.getLabels())
                    && existingRightDescriptor.getLabels().containsAll(newRightDescriptor.getLabels());
            if (!doLeftLabelsMatch || !doRightLabelsMatch) {
                return false;
            }

            // ...second, our edge labels...
            EdgeDescriptor existingEdgeDescriptor = e.getEdgeDescriptor();
            boolean doEdgeLabelsMatch = newEdgeDescriptor.getLabels().containsAll(existingEdgeDescriptor.getLabels())
                    && existingEdgeDescriptor.getLabels().containsAll(newEdgeDescriptor.getLabels());
            if (!doEdgeLabelsMatch) {
                return false;
            }

            // ...finally, our edge directions.
            return newEdgeDescriptor.getElementDirection() == existingEdgeDescriptor.getElementDirection();
        });
        if (!hasDuplicate) {
            decompositionExprs.add(decompositionExpr);
        }
    }

    public void markPropertiesAsUsed() {
        for (EdgePatternExpr decomposition : decompositionExprs) {
            decomposition.getLeftVertex().markPropertiesAsUsed();
            decomposition.getRightVertex().markPropertiesAsUsed();
        }
        leftPatternExpr.markPropertiesAsUsed();
        rightPatternExpr.markPropertiesAsUsed();
        arePropertiesUsed = true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftPatternExpr, rightPatternExpr, pathDescriptor, decompositionExprs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PathPatternExpr)) {
            return false;
        }
        PathPatternExpr that = (PathPatternExpr) o;
        return Objects.equals(leftPatternExpr, that.leftPatternExpr)
                && Objects.equals(rightPatternExpr, that.rightPatternExpr)
                && Objects.equals(pathDescriptor, that.pathDescriptor)
                && Objects.equals(decompositionExprs, that.decompositionExprs);
    }

    @Override
    public String toString() {
        String leadingEdgeString = null;
        String trailingEdgeString = null;
        switch (pathDescriptor.getElementDirection()) {
            case LEFT_TO_RIGHT:
                leadingEdgeString = "-[";
                trailingEdgeString = "]->";
                break;
            case RIGHT_TO_LEFT:
                leadingEdgeString = "<-[";
                trailingEdgeString = "]-";
                break;
            case UNDIRECTED:
                leadingEdgeString = "-[";
                trailingEdgeString = "]-";
                break;
        }
        return String.format("%s%s%s", leadingEdgeString, pathDescriptor, trailingEdgeString);
    }

    @Override
    public void setSourceLocation(SourceLocation sourceLoc) {
        decompositionExprs.forEach(e -> e.setSourceLocation(sourceLoc));
        super.setSourceLocation(sourceLoc);
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public PatternType getPatternType() {
        return PatternType.PATH_PATTERN;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }
}
