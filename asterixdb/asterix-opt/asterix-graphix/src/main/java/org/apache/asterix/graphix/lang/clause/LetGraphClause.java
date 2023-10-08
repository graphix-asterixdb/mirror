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
package org.apache.asterix.graphix.lang.clause;

import java.util.Objects;

import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * A container for {@link GraphConstructor}, to be referenced only in {@link FromGraphTerm} nodes. The actual expression
 * bound to this {@link VariableExpr} is a NULL literal.
 */
public class LetGraphClause extends LetClause {
    private final GraphConstructor graphConstructor;

    public LetGraphClause(VariableExpr varExpr, GraphConstructor graphConstructor) {
        // Note: this a bit hacky, but this LET-CLAUSE is not supposed to bind a traditional expression.
        super(varExpr, new LiteralExpr(NullLiteral.INSTANCE));
        this.graphConstructor = Objects.requireNonNull(graphConstructor);
    }

    public GraphConstructor getGraphConstructor() {
        return graphConstructor;
    }

    public GraphIdentifier createGraphIdentifier(MetadataProvider metadataProvider) {
        String graphName = SqlppVariableUtil.toUserDefinedName(getVarExpr().getVar().getValue());
        return new GraphIdentifier(metadataProvider.getDefaultDataverseName(), graphName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LetGraphClause that = (LetGraphClause) o;
        return Objects.equals(graphConstructor, that.graphConstructor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), graphConstructor);
    }
}
