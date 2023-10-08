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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query vertex pattern instance.
 * <ul>
 *   <li>A query vertex, in addition to the fields in {@link VertexDescriptor}, maintains a set of the following:
 *   <ol>
 *     <li>A {@link GraphElementDeclaration} that contains the body from the corresponding
 *     {@link org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor}.</li>
 *     <li>A {@link Vertex} that contains the key fields associated with the aforementioned mapping.</li>
 *   </ol></li>
 *   <li>A query vertex may be implicitly correlated with another query vertex. Users should set this vertex with
 *   {@link #setParentVertexExpr(VertexPatternExpr)} and retrieve this vertex with {@link #getParentVertexExpr()}.</li>
 *   <li>A query vertex may be associated with a {@link PathPatternExpr}. Users should set this association with
 *   {@link #setParentPathExpr(PathPatternExpr)} and retrieve this path with {@link #getParentPathExpr()}.</li>
 *   <li>A query vertex may not require its body to be introduced. Users should toggle this setting with
 *   {@link #markPropertiesAsUsed()} and view this setting with {@link #arePropertiesUsed()}.</li>
 * </ul>
 *
 * @see VertexDescriptor
 */
public class VertexPatternExpr extends AbstractExpression implements IPatternConstruct {
    private final VertexDescriptor vertexDescriptor;

    // We require the following for lowering.
    private final Set<GraphElementDeclaration> declarationSet;
    private final Set<Vertex> vertexInfoSet;

    // The following are set as we walk / rewrite our AST.
    private VertexPatternExpr parentVertexExpr;
    private PathPatternExpr parentPathExpr;
    private boolean arePropertiesUsed = false;

    public VertexPatternExpr(VertexDescriptor vertexDescriptor) {
        this.vertexDescriptor = Objects.requireNonNull(vertexDescriptor);
        this.declarationSet = new LinkedHashSet<>();
        this.vertexInfoSet = new LinkedHashSet<>();
    }

    public VertexDescriptor getVertexDescriptor() {
        return vertexDescriptor;
    }

    public VertexPatternExpr getParentVertexExpr() {
        return parentVertexExpr;
    }

    public PathPatternExpr getParentPathExpr() {
        return parentPathExpr;
    }

    public Set<GraphElementDeclaration> getDeclarationSet() {
        return Collections.unmodifiableSet(declarationSet);
    }

    public Set<Vertex> getVertexInfoSet() {
        return Collections.unmodifiableSet(vertexInfoSet);
    }

    public List<List<String>> getKeyFieldNames(ElementLabel label) {
        return vertexInfoSet.stream().filter(v -> v.getLabel().equals(label)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Label not found!")).getPrimaryKeyFieldNames();
    }

    public boolean arePropertiesUsed() {
        return arePropertiesUsed;
    }

    public void setParentVertexExpr(VertexPatternExpr parentVertexExpr) {
        this.parentVertexExpr = parentVertexExpr;
    }

    public void setParentPathExpr(PathPatternExpr parentPathExpr) {
        this.parentPathExpr = parentPathExpr;
    }

    public void addVertexDeclaration(GraphElementDeclaration vertexDeclaration) {
        this.declarationSet.add(vertexDeclaration);
    }

    public void addVertexInfo(Vertex vertexInfo) {
        this.vertexInfoSet.add(vertexInfo);
    }

    public void markPropertiesAsUsed() {
        this.arePropertiesUsed = true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexDescriptor, parentVertexExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof VertexPatternExpr)) {
            return false;
        }
        VertexPatternExpr that = (VertexPatternExpr) object;
        return Objects.equals(this.vertexDescriptor, that.vertexDescriptor)
                && Objects.equals(this.parentVertexExpr, that.parentVertexExpr);
    }

    @Override
    public String toString() {
        return String.format("(%s)", vertexDescriptor);
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public PatternType getPatternType() {
        return PatternType.VERTEX_PATTERN;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }
}
