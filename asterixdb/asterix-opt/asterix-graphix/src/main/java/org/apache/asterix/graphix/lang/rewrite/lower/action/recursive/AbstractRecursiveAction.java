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
package org.apache.asterix.graphix.lang.rewrite.lower.action.recursive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;

public abstract class AbstractRecursiveAction implements IEnvironmentAction {
    protected final PathPatternExpr pathPatternExpr;

    // The following should be modified on apply.
    protected final List<VariableExpr> producedPathVariables = new ArrayList<>();
    protected final List<VariableExpr> nextVertexVariables = new ArrayList<>();
    protected final List<VariableExpr> schemaVariables = new ArrayList<>();

    protected AbstractRecursiveAction(PathPatternExpr pathPatternExpr) {
        this.pathPatternExpr = pathPatternExpr;
    }

    protected void addSchemaToSequence(VertexPatternExpr vertexPatternExpr, LoweringEnvironment environment)
            throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        environment.acceptTransformer(clauseSequence -> {
            GraphElementDeclaration declaration = vertexPatternExpr.getDeclarationSet().iterator().next();
            int schemaId = environment.getIntegerIdentifierFor(declaration.getIdentifier());
            VariableExpr schemaVariable = new VariableExpr(graphixRewritingContext.newVariable());
            LiteralExpr schemaIdExpr = new LiteralExpr(new IntegerLiteral(schemaId));
            LetClause letClause = new LetClause(schemaVariable, schemaIdExpr);
            letClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
            clauseSequence.addNonRepresentativeClause(letClause);
            schemaVariables.add(schemaVariable);
        });
    }

    public List<VariableExpr> getProducedPathVariables() {
        return Collections.unmodifiableList(producedPathVariables);
    }

    public List<VariableExpr> getNextVertexVariables() {
        return Collections.unmodifiableList(nextVertexVariables);
    }

    public List<VariableExpr> getSchemaVariables() {
        return Collections.unmodifiableList(schemaVariables);
    }
}
