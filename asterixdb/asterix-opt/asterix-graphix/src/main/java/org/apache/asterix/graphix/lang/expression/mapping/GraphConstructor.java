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
package org.apache.asterix.graphix.lang.expression.mapping;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.collections4.iterators.IteratorChain;

/**
 * An expression which describes the schema of a graph, containing:
 * <ul>
 *  <li>A list of vertices ({@link VertexConstructor}).</li>
 *  <li>A list of edges ({@link EdgeConstructor}) that connect the aforementioned vertices.</li>
 * </ul>
 */
public class GraphConstructor extends AbstractExpression implements Iterable<IMappingConstruct> {
    private final List<VertexConstructor> vertexConstructors;
    private final List<EdgeConstructor> edgeConstructors;

    public GraphConstructor(List<VertexConstructor> vertexConstructors, List<EdgeConstructor> edgeConstructors) {
        this.vertexConstructors = vertexConstructors;
        this.edgeConstructors = edgeConstructors;
    }

    public List<VertexConstructor> getVertexElements() {
        return Collections.unmodifiableList(vertexConstructors);
    }

    public List<EdgeConstructor> getEdgeElements() {
        return Collections.unmodifiableList(edgeConstructors);
    }

    @Override
    public Iterator<IMappingConstruct> iterator() {
        IteratorChain<IMappingConstruct> iteratorChain = new IteratorChain<>();
        iteratorChain.addIterator(vertexConstructors.iterator());
        iteratorChain.addIterator(edgeConstructors.iterator());
        return iteratorChain;
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexConstructors, edgeConstructors);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphConstructor)) {
            return false;
        }
        GraphConstructor that = (GraphConstructor) object;
        return vertexConstructors.equals(that.vertexConstructors) && edgeConstructors.equals(that.edgeConstructors);
    }
}
