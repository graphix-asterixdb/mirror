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
 * An edge constructor (not be confused with a query edge) is composed of the following:
 * <ul>
 *  <li>An AST containing the edge body expression, as well as the raw body string itself.</li>
 *  <li>A single edge label that uniquely identifies the edge.</li>
 *  <li>A single label that denotes the source vertices of this edge, as well as another label that denotes the
 *  destination vertices of this edge.</li>
 *  <li>A list of source key fields, used in the JOIN clause with the corresponding source vertices.</li>
 *  <li>A list of destination key fields, used in the JOIN clause with the corresponding destination vertices.</li>
 * </ul>
 */
public class EdgeConstructor extends AbstractLangExpression implements IMappingConstruct {
    private final List<Integer> destinationKeySourceIndicators;
    private final List<Integer> sourceKeySourceIndicators;

    private final List<List<String>> destinationKeyFields;
    private final List<List<String>> sourceKeyFields;

    private final ElementLabel destinationLabel, edgeLabel, sourceLabel;
    private final Expression expression;
    private final String definition;

    public EdgeConstructor(ElementLabel edgeLabel, ElementLabel destinationLabel, ElementLabel sourceLabel,
            List<List<String>> destinationKeyFields, List<Integer> destinationKeySourceIndicators,
            List<List<String>> sourceKeyFields, List<Integer> sourceKeySourceIndicators, Expression expression,
            String definition) {
        this.destinationKeySourceIndicators = destinationKeySourceIndicators;
        this.sourceKeySourceIndicators = sourceKeySourceIndicators;
        this.destinationKeyFields = destinationKeyFields;
        this.sourceKeyFields = sourceKeyFields;
        this.destinationLabel = destinationLabel;
        this.edgeLabel = edgeLabel;
        this.sourceLabel = sourceLabel;
        this.expression = expression;
        this.definition = definition;
    }

    public List<Integer> getDestinationKeySourceIndicators() {
        return destinationKeySourceIndicators;
    }

    public List<Integer> getSourceKeySourceIndicators() {
        return sourceKeySourceIndicators;
    }

    public List<List<String>> getDestinationKeyFields() {
        return destinationKeyFields;
    }

    public List<List<String>> getSourceKeyFields() {
        return sourceKeyFields;
    }

    public ElementLabel getDestinationLabel() {
        return destinationLabel;
    }

    public ElementLabel getEdgeLabel() {
        return edgeLabel;
    }

    public ElementLabel getSourceLabel() {
        return sourceLabel;
    }

    public Expression getExpression() {
        return expression;
    }

    public String getDefinition() {
        return definition;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.EDGE_MAPPING;
    }

    @Override
    public String toString() {
        String edgeBodyPattern = "[:" + edgeLabel + "]";
        String sourceNodePattern = "(:" + sourceLabel + ")";
        String destinationNodePattern = "(:" + destinationLabel + ")";
        String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
        return edgePattern + " AS " + definition;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destinationKeyFields, sourceKeyFields, destinationLabel, edgeLabel, sourceLabel, expression,
                definition);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof EdgeConstructor)) {
            return false;
        }
        EdgeConstructor that = (EdgeConstructor) object;
        return destinationKeySourceIndicators.equals(that.destinationKeySourceIndicators)
                && sourceKeySourceIndicators.equals(that.sourceKeySourceIndicators)
                && destinationKeyFields.equals(that.destinationKeyFields)
                && sourceKeyFields.equals(that.sourceKeyFields) && destinationLabel.equals(that.destinationLabel)
                && edgeLabel.equals(that.edgeLabel) && sourceLabel.equals(that.sourceLabel)
                && expression.equals(that.expression) && definition.equals(that.definition);
    }
}