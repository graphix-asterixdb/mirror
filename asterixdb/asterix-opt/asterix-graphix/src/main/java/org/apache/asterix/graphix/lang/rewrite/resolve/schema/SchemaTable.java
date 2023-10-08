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
package org.apache.asterix.graphix.lang.rewrite.resolve.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;

/**
 * A collection of ground truths, derived from the graph schema (either a {@link Graph} or {@link GraphConstructor}).
 * The purpose of this table is to answer boolean queries (in the form of {@link SchemaQuery}).
 */
public class SchemaTable extends HashSet<EdgeIdentifier> {
    private final Set<ElementLabel> vertexLabels = new HashSet<>();
    private final Set<ElementLabel> edgeLabels = new HashSet<>();
    private final GraphIdentifier graphIdentifier;

    public SchemaTable(Graph graph) {
        this.graphIdentifier = graph.getIdentifier();
        for (Vertex vertex : graph.getGraphSchema().getVertices()) {
            vertexLabels.add(vertex.getLabel());
        }
        for (Edge edge : graph.getGraphSchema().getEdges()) {
            edgeLabels.add(edge.getLabel());
            this.add(edge.getIdentifier());
        }
    }

    public SchemaTable(GraphConstructor graphConstructor, GraphIdentifier graphIdentifier) {
        this.graphIdentifier = graphIdentifier;
        for (VertexConstructor vertexElement : graphConstructor.getVertexElements()) {
            vertexLabels.add(vertexElement.getLabel());
        }
        for (EdgeConstructor edgeElement : graphConstructor.getEdgeElements()) {
            edgeLabels.add(edgeElement.getEdgeLabel());

            // Build our own edge identifier here.
            EdgeIdentifier edgeIdentifier = new EdgeIdentifier(graphIdentifier, edgeElement.getSourceLabel(),
                    edgeElement.getEdgeLabel(), edgeElement.getDestinationLabel());
            this.add(edgeIdentifier);
        }
    }

    public Set<ElementLabel> getEdgeLabels() {
        return Collections.unmodifiableSet(edgeLabels);
    }

    public Set<ElementLabel> getVertexLabels() {
        return Collections.unmodifiableSet(vertexLabels);
    }

    public boolean answerQuery(SchemaQuery schemaQuery) {
        return schemaQuery.getIdentifiers(graphIdentifier).stream().allMatch(this::contains);
    }
}
