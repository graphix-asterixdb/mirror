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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for an abstract pattern instance. All patterns possess the following:
 * <ul>
 *  <li>A set of labels.</li>
 *  <li>A variable associated with the pattern.</li>
 * </ul>
 */
public abstract class AbstractDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<ElementLabel> labels;
    private VariableExpr variableExpr;
    private boolean isVariableGenerated = false;

    public AbstractDescriptor(Set<ElementLabel> labels, VariableExpr variableExpr) {
        this.labels = labels;
        this.variableExpr = variableExpr;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public Set<ElementLabel> getLabels() {
        return Collections.unmodifiableSet(labels);
    }

    public boolean isVariableGenerated() {
        return isVariableGenerated;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
        this.isVariableGenerated = true;
    }

    public void replaceLabels(Collection<ElementLabel> newLabelSet) {
        this.labels.clear();
        this.labels.addAll(newLabelSet);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractDescriptor)) {
            return false;
        }
        AbstractDescriptor that = (AbstractDescriptor) o;
        return Objects.equals(labels, that.labels) && Objects.equals(variableExpr, that.variableExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, variableExpr);
    }
}
