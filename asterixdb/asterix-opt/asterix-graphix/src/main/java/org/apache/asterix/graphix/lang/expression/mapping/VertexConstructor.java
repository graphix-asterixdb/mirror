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

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractLangExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A vertex constructor (not be confused with a query vertex) is composed of the following:
 * <ul>
 *  <li>An AST containing the vertex body expression, as well as the raw body string itself.</li>
 *  <li>A single vertex label that uniquely identifies the vertex.</li>
 *  <li>A list of primary key fields, used in the JOIN clause with edges.</li>
 * </ul>
 */
public class VertexConstructor extends AbstractLangExpression implements IMappingConstruct {
    private final List<Integer> primaryKeySourceIndicators;
    private final List<List<String>> primaryKeyFields;
    private final Expression expression;
    private final ElementLabel label;
    private final String definition;

    public VertexConstructor(ElementLabel label, List<List<String>> primaryKeyFields,
            List<Integer> primaryKeySourceIndicators, Expression expression, String definition) {
        this.primaryKeySourceIndicators = primaryKeySourceIndicators;
        this.primaryKeyFields = primaryKeyFields;
        this.expression = expression;
        this.definition = definition;
        this.label = label;
    }

    public List<List<String>> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public List<Integer> getPrimaryKeySourceIndicators() {
        return primaryKeySourceIndicators;
    }

    public Expression getExpression() {
        return expression;
    }

    public String getDefinition() {
        return definition;
    }

    public ElementLabel getLabel() {
        return label;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.VERTEX_MAPPING;
    }

    @Override
    public String toString() {
        return "(:" + label + ") AS " + definition;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKeyFields, expression, definition, label);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof VertexConstructor)) {
            return false;
        }
        VertexConstructor that = (VertexConstructor) object;
        return primaryKeySourceIndicators.equals(that.primaryKeySourceIndicators)
                && primaryKeyFields.equals(that.primaryKeyFields) && expression.equals(that.expression)
                && definition.equals(that.definition) && label.equals(that.label);
    }
}
