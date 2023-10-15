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
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.NavigationJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.PropertiesRequiredAnnotation;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class RecursiveSequenceAction extends AbstractRecursiveAction {
    private final List<VariableExpr> inputPathVariables = new ArrayList<>();
    private final List<VariableExpr> inputVertexVariables = new ArrayList<>();
    private final List<VariableExpr> inputSchemaVariables = new ArrayList<>();
    private final List<ClauseSequence> loweredDecompositions = new ArrayList<>();
    private final IEnvironmentAction.Callback loweringCallback;

    public RecursiveSequenceAction(Callback loweringCallback, PathPatternExpr pathPatternExpr) {
        super(pathPatternExpr);
        this.loweringCallback = loweringCallback;
    }

    public List<VariableExpr> getInputVertexVariables() {
        return Collections.unmodifiableList(inputVertexVariables);
    }

    public List<VariableExpr> getInputPathVariables() {
        return Collections.unmodifiableList(inputPathVariables);
    }

    public List<VariableExpr> getInputSchemaVariables() {
        return Collections.unmodifiableList(inputSchemaVariables);
    }

    public List<ClauseSequence> getLoweredDecompositions() {
        return Collections.unmodifiableList(loweredDecompositions);
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();

        // Collect our recursive decompositions and next variables.
        VariableExpr pathRightVariable = pathPatternExpr.getRightVertex().getVertexDescriptor().getVariableExpr();
        boolean isEvaluatingLeftToRight = !environment.isElementIntroduced(pathRightVariable);
        for (EdgePatternExpr decomposition : pathPatternExpr.getDecompositions()) {
            VertexPatternExpr leftVertexExpr = decomposition.getLeftVertex();
            VertexPatternExpr rightVertexExpr = decomposition.getRightVertex();
            VertexDescriptor rightDescriptor = rightVertexExpr.getVertexDescriptor();
            VertexDescriptor leftDescriptor = leftVertexExpr.getVertexDescriptor();
            VariableExpr edgeVariable = decomposition.getEdgeDescriptor().getVariableExpr();
            VariableExpr rightVariable = rightDescriptor.getVariableExpr();
            VariableExpr leftVariable = leftDescriptor.getVariableExpr();

            // We will prepare our input and output vertex variables here.
            if (isEvaluatingLeftToRight) {
                aliasLookupTable.addIterationAlias(leftVariable, leftVariable);
                aliasLookupTable.addJoinAlias(leftVariable, leftVariable);
                inputVertexVariables.add(leftVariable);
                nextVertexVariables.add(rightVariable);

            } else {
                aliasLookupTable.addIterationAlias(rightVariable, rightVariable);
                aliasLookupTable.addJoinAlias(rightVariable, rightVariable);
                inputVertexVariables.add(rightVariable);
                nextVertexVariables.add(leftVariable);
            }

            // Prepare our input schema variable here.
            VariableExpr inputSchemaVariable = new VariableExpr(graphixRewritingContext.newVariable());
            inputSchemaVariables.add(inputSchemaVariable);

            // Prepare our input and output path variables here.
            VariableExpr existingPathVariable = pathPatternExpr.getPathDescriptor().getVariableExpr();
            VariableExpr inputPathVariable = graphixRewritingContext.getGraphixVariableCopy(existingPathVariable);
            VariableExpr newPathVariable = graphixRewritingContext.getGraphixVariableCopy(existingPathVariable);
            inputPathVariables.add(inputPathVariable);

            // If we are given an INLJ hint to our path, then annotate this decomposition.
            NavigationJoinAnnotation navigationHint = pathPatternExpr.findHint(NavigationJoinAnnotation.class);
            if (navigationHint != null) {
                navigationHint.annotateDecomposition(decomposition);
            }

            // Lower our sequence.
            environment.pushClauseSequence();
            loweringCallback.invoke(decomposition, null);
            addSchemaToSequence(isEvaluatingLeftToRight ? rightVertexExpr : leftVertexExpr, environment);

            // Eagerly filter out paths that are longer than our specified maximum.
            PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
            if (pathDescriptor.getMaximumHops() != null) {
                LiteralExpr maximumHopCountExpr = new LiteralExpr(new IntegerLiteral(pathDescriptor.getMaximumHops()));
                maximumHopCountExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
                FunctionIdentifier functionIdentifier = GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER;
                FunctionSignature functionSignature = new FunctionSignature(functionIdentifier);
                List<Expression> functionArgs = List.of(new VariableExpr(inputPathVariable.getVar()));
                CallExpr edgeCountFromHeaderExpr = new CallExpr(functionSignature, functionArgs);
                edgeCountFromHeaderExpr.setSourceLocation(pathPatternExpr.getSourceLocation());

                // Our bounds are inclusive, but this WHERE appears before the APPEND.
                OperatorExpr operatorExpr = new OperatorExpr();
                operatorExpr.addOperator(OperatorType.LT);
                operatorExpr.addOperand(edgeCountFromHeaderExpr);
                operatorExpr.addOperand(maximumHopCountExpr);
                WhereClause whereClause = new WhereClause(operatorExpr);
                environment.acceptTransformer(c -> c.addNonRepresentativeClause(whereClause));
            }

            // Append to our working path using our traveled-to-vertex and edge.
            FunctionIdentifier functionIdentifier = GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
            FunctionSignature functionSignature = new FunctionSignature(functionIdentifier);
            List<Expression> functionArgs = new ArrayList<>();
            functionArgs.add(aliasLookupTable.getJoinAlias(isEvaluatingLeftToRight ? rightVariable : leftVariable));
            functionArgs.add(aliasLookupTable.getJoinAlias(edgeVariable));
            functionArgs.add(inputPathVariable);
            CallExpr callExpr = new CallExpr(functionSignature, functionArgs);
            callExpr.setSourceLocation(pathPatternExpr.getSourceLocation());
            if (pathPatternExpr.arePropertiesUsed()) {
                callExpr.addHint(PropertiesRequiredAnnotation.INSTANCE);
            }
            LetClause newPathBinding = new LetClause(newPathVariable, callExpr);
            newPathBinding.setSourceLocation(pathPatternExpr.getSourceLocation());
            environment.acceptTransformer(c -> c.addNonRepresentativeClause(newPathBinding));

            // Add our representative path.
            VariableExpr producedPathVariable = graphixRewritingContext.getGraphixVariableCopy(existingPathVariable);
            producedPathVariables.add(producedPathVariable);
            environment.acceptTransformer(c -> c.addPathBinding(producedPathVariable, newPathVariable));
            loweredDecompositions.add(environment.popClauseSequence());
            aliasLookupTable.addIterationAlias(producedPathVariable, newPathVariable);
        }
    }
}
