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
package org.apache.asterix.graphix.metadata.entity.schema;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of a vertex. A vertex consists of the following:
 * <ul>
 *  <li>A {@link VertexIdentifier}, to uniquely identify the vertex across other graph elements.</li>
 *  <li>A list of primary key fields, associated with the definition body.</li>
 *  <li>A SQL++ string denoting the definition body.</li>
 * </ul>
 */
public class Vertex implements IElement {
    private static final long serialVersionUID = 1L;

    private final VertexIdentifier identifier;
    private final List<List<String>> primaryKeyFieldNames;
    private final String definitionBody;

    /**
     * Use {@link Schema.Builder} to build Vertex instances instead of this constructor.
     */
    Vertex(VertexIdentifier identifier, List<List<String>> primaryKeyFieldNames, String definitionBody) {
        this.identifier = Objects.requireNonNull(identifier);
        this.primaryKeyFieldNames = primaryKeyFieldNames;
        this.definitionBody = Objects.requireNonNull(definitionBody);
    }

    public List<List<String>> getPrimaryKeyFieldNames() {
        return primaryKeyFieldNames;
    }

    @Override
    public VertexIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public ElementLabel getLabel() {
        return identifier.getVertexLabel();
    }

    @Override
    public String getDefinitionBody() {
        return definitionBody;
    }

    @Override
    public String toString() {
        return "(:" + getLabel() + ") AS " + definitionBody;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Vertex))
            return false;
        Vertex vertex = (Vertex) o;
        return Objects.equals(identifier, vertex.identifier)
                && Objects.equals(primaryKeyFieldNames, vertex.primaryKeyFieldNames)
                && Objects.equals(definitionBody, vertex.definitionBody);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, primaryKeyFieldNames, definitionBody);
    }
}
