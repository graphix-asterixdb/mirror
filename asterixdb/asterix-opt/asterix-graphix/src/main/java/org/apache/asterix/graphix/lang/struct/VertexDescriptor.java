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

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query vertex instance. A query vertex, in addition to the two fields from
 * {@link AbstractDescriptor}, has the following:
 * <ul>
 *  <li>A filter expression (allowed to be NULL).</li>
 * </ul>
 */
public class VertexDescriptor extends AbstractDescriptor {
    private static final long serialVersionUID = 1L;

    private Expression filterExpr;

    public VertexDescriptor(VariableExpr variableExpr, Set<ElementLabel> labels, Expression filterExpr) {
        super(labels, variableExpr);
        this.filterExpr = filterExpr;
    }

    public Expression getFilterExpr() {
        return filterExpr;
    }

    public void setFilterExpr(Expression filterExpr) {
        if (this.filterExpr != null) {
            throw new IllegalStateException("Cannot overwrite existing filter expression!");
        }
        this.filterExpr = filterExpr;
    }

    public void removeFilterExpr() {
        this.filterExpr = null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filterExpr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VertexDescriptor)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VertexDescriptor that = (VertexDescriptor) o;
        return Objects.equals(filterExpr, that.filterExpr);
    }

    @Override
    public String toString() {
        String labelsString = getLabels().stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (getVariableExpr() != null) ? getVariableExpr().getVar().toString() : "";
        String filterString = (filterExpr != null) ? (" WHERE " + filterExpr + " ") : "";
        return String.format("%s:%s%s", variableString, labelsString, filterString);
    }
}
