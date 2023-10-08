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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query edge instance. A query edge, in addition to the two fields from {@link AbstractDescriptor},
 * has the following:
 * <ul>
 *  <li>An element direction (left to right, right to left, or undirected).</li>
 *  <li>A filter expression (allowed to be NULL).</li>
 * </ul>
 */
public class EdgeDescriptor extends AbstractDescriptor {
    private static final long serialVersionUID = 1L;

    private Expression filterExpr;
    private ElementDirection elementDirection;

    public EdgeDescriptor(ElementDirection elementDirection, VariableExpr variableExpr, Set<ElementLabel> labels,
            Expression filterExpr) {
        super(labels, variableExpr);
        this.elementDirection = elementDirection;
        this.filterExpr = filterExpr;
    }

    public ElementDirection getElementDirection() {
        return elementDirection;
    }

    public Expression getFilterExpr() {
        return filterExpr;
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

    public void setFilterExpr(Expression filterExpr) {
        this.filterExpr = filterExpr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filterExpr, elementDirection);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EdgeDescriptor)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        EdgeDescriptor that = (EdgeDescriptor) o;
        return Objects.equals(filterExpr, that.filterExpr) && elementDirection == that.elementDirection;
    }

    @Override
    public String toString() {
        String labelsString = getLabels().stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (getVariableExpr() != null) ? getVariableExpr().getVar().toString() : "";
        String filterString = (filterExpr == null) ? "" : (" WHERE " + filterExpr + " ");
        return String.format("%s:(%s)%s", variableString, labelsString, filterString);
    }
}
