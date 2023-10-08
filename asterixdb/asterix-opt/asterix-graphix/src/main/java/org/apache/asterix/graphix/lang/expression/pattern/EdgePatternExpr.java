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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query edge pattern instance.
 * <ul>
 *   <li>A query edge, in addition the fields in {@link EdgeDescriptor}, maintains a set of the following:
 *   <ol>
 *     <li>A {@link GraphElementDeclaration} that contains the body from the corresponding
 *     {@link org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor}.</li>
 *     <li>An {@link Edge} that contains the key fields associated with the aforementioned mapping.</li>
 *   </ol></li>
 *   <li>A query edge associates a left {@link VertexPatternExpr} and a right {@link VertexPatternExpr}.</li>
 *   <li>A query edge may be associated with a {@link PathPatternExpr}. Users should set this association with
 *   {@link #setParentPathExpr(PathPatternExpr)} and retrieve this path with {@link #getParentPathExpr()}.</li>
 * </ul>
 *
 * @see EdgeDescriptor
 */
public class EdgePatternExpr extends AbstractExpression implements IPatternConstruct {
    private final EdgeDescriptor edgeDescriptor;
    private final VertexPatternExpr leftVertex;
    private final VertexPatternExpr rightVertex;

    // We require the following for lowering.
    private final Set<GraphElementDeclaration> declarationSet;
    private final Set<Edge> edgeInfoSet;

    // The following is set as we walk / rewrite our AST.
    private PathPatternExpr parentPathExpr;

    public EdgePatternExpr(VertexPatternExpr leftVertex, VertexPatternExpr rightVertex, EdgeDescriptor edgeDescriptor) {
        this.leftVertex = leftVertex;
        this.rightVertex = rightVertex;
        this.edgeDescriptor = edgeDescriptor;
        this.declarationSet = new LinkedHashSet<>();
        this.edgeInfoSet = new LinkedHashSet<>();
    }

    public VertexPatternExpr getLeftVertex() {
        return leftVertex;
    }

    public VertexPatternExpr getRightVertex() {
        return rightVertex;
    }

    public EdgeDescriptor getEdgeDescriptor() {
        return edgeDescriptor;
    }

    public PathPatternExpr getParentPathExpr() {
        return parentPathExpr;
    }

    public Set<GraphElementDeclaration> getDeclarationSet() {
        return Collections.unmodifiableSet(declarationSet);
    }

    public Set<Edge> getEdgeInfoSet() {
        return Collections.unmodifiableSet(edgeInfoSet);
    }

    public List<List<String>> getLeftKeyFieldNames(ElementLabel label) {
        List<List<String>> keyFieldNames = new ArrayList<>();
        if (edgeDescriptor.getElementDirection() != ElementDirection.RIGHT_TO_LEFT) {
            keyFieldNames.addAll(edgeInfoSet.stream().filter(e -> e.getSourceLabel().equals(label)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Label not found!")).getSourceKeyFieldNames());
        }
        if (edgeDescriptor.getElementDirection() != ElementDirection.LEFT_TO_RIGHT) {
            keyFieldNames.addAll(edgeInfoSet.stream().filter(e -> e.getDestinationLabel().equals(label)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Label not found!")).getDestinationKeyFieldNames());
        }
        return keyFieldNames;
    }

    public List<List<String>> getRightKeyFieldNames(ElementLabel label) {
        List<List<String>> keyFieldNames = new ArrayList<>();
        if (edgeDescriptor.getElementDirection() != ElementDirection.RIGHT_TO_LEFT) {
            keyFieldNames.addAll(edgeInfoSet.stream().filter(e -> e.getDestinationLabel().equals(label)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Label not found!")).getDestinationKeyFieldNames());
        }
        if (edgeDescriptor.getElementDirection() != ElementDirection.LEFT_TO_RIGHT) {
            keyFieldNames.addAll(edgeInfoSet.stream().filter(e -> e.getSourceLabel().equals(label)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Label not found!")).getSourceKeyFieldNames());
        }
        return keyFieldNames;
    }

    public void setParentPathExpr(PathPatternExpr parentPathExpr) {
        this.parentPathExpr = parentPathExpr;
    }

    public void addEdgeDeclaration(GraphElementDeclaration edgeDeclaration) {
        this.declarationSet.add(edgeDeclaration);
    }

    public void addEdgeInfo(Edge edgeInfo) {
        this.edgeInfoSet.add(edgeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeDescriptor, leftVertex, rightVertex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EdgePatternExpr)) {
            return false;
        }
        EdgePatternExpr that = (EdgePatternExpr) o;
        return Objects.equals(edgeDescriptor, that.edgeDescriptor) && Objects.equals(leftVertex, that.leftVertex)
                && Objects.equals(rightVertex, that.rightVertex);
    }

    @Override
    public String toString() {
        String leadingEdgeString = null;
        String trailingEdgeString = null;
        switch (edgeDescriptor.getElementDirection()) {
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
        return String.format("%s%s%s", leadingEdgeString, edgeDescriptor, trailingEdgeString);
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public PatternType getPatternType() {
        return PatternType.EDGE_PATTERN;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }
}
