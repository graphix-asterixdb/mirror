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
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.PropertiesRequiredAnnotation;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AnchorSequenceAction extends AbstractRecursiveAction {
    private final List<VariableExpr> auxiliaryVariables = new ArrayList<>();
    private final List<LetClause> nextVertexBindings = new ArrayList<>();

    public AnchorSequenceAction(PathPatternExpr pathPatternExpr) {
        super(pathPatternExpr);
    }

    public List<VariableExpr> getAuxiliaryVariables() {
        return Collections.unmodifiableList(auxiliaryVariables);
    }

    public List<LetClause> getNextVertexBindings() {
        return Collections.unmodifiableList(nextVertexBindings);
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();

        // Determine our source and destination vertices.
        VertexPatternExpr leftVertex = pathPatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = pathPatternExpr.getRightVertex();
        VertexDescriptor rightDescriptor = rightVertex.getVertexDescriptor();
        VertexDescriptor leftDescriptor = leftVertex.getVertexDescriptor();
        boolean isEvaluatingLeftToRight = !environment.isElementIntroduced(rightDescriptor.getVariableExpr());
        VertexDescriptor sourceDescriptor = isEvaluatingLeftToRight ? leftDescriptor : rightDescriptor;
        VertexDescriptor destDescriptor = isEvaluatingLeftToRight ? rightDescriptor : leftDescriptor;

        // First, our source vertex should already be introduced. This is also going to be our next vertex.
        VariableExpr destVariable = destDescriptor.getVariableExpr();
        VariableExpr sourceVariable = sourceDescriptor.getVariableExpr();
        VariableExpr sourceJoinAlias = aliasLookupTable.getJoinAlias(sourceVariable);
        VariableExpr newDestVariable = graphixRewritingContext.getGraphixVariableCopy(destVariable);
        environment.acceptTransformer(clauseSequence -> {
            LetClause destVertexBinding = new LetClause(newDestVariable, sourceJoinAlias);
            destVertexBinding.setSourceLocation(pathPatternExpr.getSourceLocation());
            environment.acceptTransformer(c -> c.addNonRepresentativeClause(destVertexBinding));
            nextVertexBindings.add(destVertexBinding);
        });
        nextVertexVariables.add(newDestVariable);
        auxiliaryVariables.add(sourceJoinAlias);

        // Second, build an initial path using our destination variable.
        FunctionIdentifier functionIdentifier = GraphixFunctionIdentifiers.CREATE_NEW_ZERO_HOP_PATH;
        FunctionSignature functionSignature = new FunctionSignature(functionIdentifier);
        List<Expression> functionArgs = List.of(new VariableExpr(newDestVariable.getVar()));
        CallExpr callExpr = new CallExpr(functionSignature, functionArgs);
        callExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
        if (pathPatternExpr.arePropertiesUsed()) {
            callExpr.addHint(PropertiesRequiredAnnotation.INSTANCE);
        }
        VariableExpr existingPathVariable = pathPatternExpr.getPathDescriptor().getVariableExpr();
        VariableExpr newPathVariable = graphixRewritingContext.getGraphixVariableCopy(existingPathVariable);
        LetClause newPathBinding = new LetClause(newPathVariable, callExpr);
        newPathBinding.setSourceLocation(pathPatternExpr.getSourceLocation());
        environment.acceptTransformer(c -> c.addNonRepresentativeClause(newPathBinding));
        producedPathVariables.add(newPathVariable);

        // Third, add our schema.
        addSchemaToSequence(isEvaluatingLeftToRight ? leftVertex : rightVertex, environment);
    }
}
