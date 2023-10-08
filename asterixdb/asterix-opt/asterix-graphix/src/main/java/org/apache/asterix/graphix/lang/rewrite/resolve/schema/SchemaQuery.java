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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Input to a {@link SchemaTable} instance, used to check whether the tuple:
 * {@literal <LEFT_VERTEX_LABEL, EDGE_LABEL, RIGHT_LABEL, EDGE_DIRECTION>} does not contradict a {@link SchemaTable}.
 * We expect all variables of the aforementioned tuple to be bound (e.g. we should not be verifying
 * {@link ElementDirection#UNDIRECTED} edges).
 */
public class SchemaQuery {
    private final ElementLabel leftLabel;
    private final ElementLabel edgeLabel;
    private final ElementLabel rightLabel;
    private final ElementDirection direction;

    public SchemaQuery(ElementLabel leftLabel, ElementLabel edgeLabel, ElementLabel rightLabel,
            ElementDirection direction) {
        this.leftLabel = Objects.requireNonNull(leftLabel);
        this.edgeLabel = Objects.requireNonNull(edgeLabel);
        this.rightLabel = Objects.requireNonNull(rightLabel);
        this.direction = Objects.requireNonNull(direction);
        if (direction == ElementDirection.UNDIRECTED) {
            throw new IllegalArgumentException("A SchemaQuery must bind all unknowns!");
        }
    }

    public List<EdgeIdentifier> getIdentifiers(GraphIdentifier graphIdentifier) {
        List<EdgeIdentifier> edgeIdentifiers = new ArrayList<>();
        if (direction != ElementDirection.RIGHT_TO_LEFT) {
            edgeIdentifiers.add(new EdgeIdentifier(graphIdentifier, leftLabel, edgeLabel, rightLabel));
        }
        if (direction != ElementDirection.LEFT_TO_RIGHT) {
            edgeIdentifiers.add(new EdgeIdentifier(graphIdentifier, rightLabel, edgeLabel, leftLabel));
        }
        return edgeIdentifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaQuery)) {
            return false;
        }
        SchemaQuery that = (SchemaQuery) o;
        return Objects.equals(leftLabel, that.leftLabel) && Objects.equals(edgeLabel, that.edgeLabel)
                && Objects.equals(rightLabel, that.rightLabel) && direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftLabel, edgeLabel, rightLabel, direction);
    }

    @Override
    public String toString() {
        switch (direction) {
            case LEFT_TO_RIGHT:
                return String.format("(%s)-[%s]->(%s)", leftLabel, edgeLabel, rightLabel);
            case RIGHT_TO_LEFT:
                return String.format("(%s)<-[%s]-(%s)", leftLabel, edgeLabel, rightLabel);
            case UNDIRECTED:
                return String.format("(%s)-[%s]-(%s)", leftLabel, edgeLabel, rightLabel);
        }
        throw new IllegalStateException();
    }
}
