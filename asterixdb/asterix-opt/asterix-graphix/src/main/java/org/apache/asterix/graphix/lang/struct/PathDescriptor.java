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
package org.apache.asterix.graphix.lang.struct;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query path instance. A query path, in addition to the two fields from {@link AbstractDescriptor},
 * has the following:
 * <ul>
 *  <li>A minimum number of hops (not allowed to be NULL).</li>
 *  <li>A maximum number of hops (allowed to be NULL, indicating an unbounded maximum).</li>
 *  <li>A path direction (left to right, right to left, or undirected).</li>
 * </ul>
 */
public class PathDescriptor extends AbstractDescriptor {
    private static final long serialVersionUID = 1L;

    private final Integer minimumHops;
    private final Integer maximumHops;
    private ElementDirection elementDirection;

    public PathDescriptor(ElementDirection elementDirection, VariableExpr variableExpr, Set<ElementLabel> edgeLabels,
            Integer minimumHops, Integer maximumHops) {
        super(edgeLabels, variableExpr);
        this.minimumHops = Objects.requireNonNull(minimumHops);
        this.maximumHops = maximumHops;
        this.elementDirection = elementDirection;
    }

    public ElementDirection getElementDirection() {
        return elementDirection;
    }

    public Integer getMinimumHops() {
        return minimumHops;
    }

    public Integer getMaximumHops() {
        return maximumHops;
    }

    public boolean setElementDirection(ElementDirection elementDirection) throws CompilationException {
        switch (this.elementDirection) {
            case LEFT_TO_RIGHT:
                if (elementDirection == ElementDirection.LEFT_TO_RIGHT) {
                    return false;
                }
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Directions cannot be removed from an edge!");
            case RIGHT_TO_LEFT:
                if (elementDirection == ElementDirection.RIGHT_TO_LEFT) {
                    return false;
                }
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Directions cannot be removed from an edge!");
            default: // case UNDIRECTED:
                this.elementDirection = elementDirection;
                return true;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minimumHops, maximumHops);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PathDescriptor)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PathDescriptor that = (PathDescriptor) o;
        return Objects.equals(minimumHops, that.minimumHops) && Objects.equals(maximumHops, that.maximumHops);
    }

    @Override
    public String toString() {
        String labelsString = getLabels().stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (getVariableExpr() != null) ? getVariableExpr().getVar().toString() : "";
        String minHopsString = ((minimumHops == null) ? "" : minimumHops.toString());
        String maxHopsString = ((maximumHops == null) ? "" : maximumHops.toString());
        String repetitionString = String.format("{%s,%s}", minHopsString, maxHopsString);
        return String.format("%s:(%s)%s", variableString, labelsString, repetitionString);
    }
}
