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
package org.apache.asterix.graphix.lang.rewrite.lower.action.finalize;

import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.DEST_VARIABLES_INDEX;
import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.PATH_VARIABLE_INDEX;
import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.SCHEMA_VARIABLE_INDEX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.recursive.AnchorSequenceAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.recursive.RecursiveSequenceAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FinalizePathPatternAction extends AbstractFinalizeAction {
    private final LoopingClause loopingClause;
    private final PathDescriptor pathDescriptor;
    private final PathPatternExpr pathPatternExpr;

    private final VariableExpr destVariable;
    private final VariableExpr pathVariable;
    private final VariableExpr schemaVariable;
    private final ElementDirection evaluationDirection;

    public FinalizePathPatternAction(PathPatternExpr pathPatternExpr, AnchorSequenceAction anchorAction,
            RecursiveSequenceAction recursiveAction, VariableExpr destVariable, VariableExpr pathVariable,
            VariableExpr schemaVariable, boolean isEvaluatingLeftToRight) {
        super(pathPatternExpr.getSourceLocation());
        this.destVariable = destVariable;
        this.schemaVariable = schemaVariable;
        this.pathVariable = pathVariable;
        this.pathPatternExpr = pathPatternExpr;
        this.pathDescriptor = pathPatternExpr.getPathDescriptor();
        this.evaluationDirection =
                isEvaluatingLeftToRight ? ElementDirection.LEFT_TO_RIGHT : ElementDirection.RIGHT_TO_LEFT;

        // Build the looping clause.
        LoopingClauseBuilder loopingClauseBuilder = new LoopingClauseBuilder(anchorAction, recursiveAction);
        loopingClauseBuilder.setOutputDestVariable(this.destVariable);
        loopingClauseBuilder.setOutputSchemaVariable(this.schemaVariable);
        loopingClauseBuilder.setOutputPathVariable(this.pathVariable);
        this.loopingClause = loopingClauseBuilder.build();
        this.loopingClause.setSourceLocation(pathPatternExpr.getSourceLocation());
    }

    public static Expression generatePathHopCountExpression(Expression pathVariableAccess) {
        FunctionSignature fs1 = new FunctionSignature(GraphixFunctionIdentifiers.PATH_EDGES);
        FunctionSignature fs2 = new FunctionSignature(BuiltinFunctions.LEN);
        CallExpr callExpr1 = new CallExpr(fs1, List.of(pathVariableAccess));
        return new CallExpr(fs2, List.of(callExpr1));
    }

    public LoopingClause getLoopingClause() {
        return loopingClause;
    }

    @Override
    protected FromGraphTerm getFromGraphTerm() {
        VertexPatternExpr leftVertex = pathPatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = pathPatternExpr.getRightVertex();
        QueryPatternExpr queryPatternExpr = new QueryPatternExpr(List.of(leftVertex, pathPatternExpr, rightVertex));
        MatchClause matchClause = new MatchClause(List.of(queryPatternExpr), MatchType.LEADING);
        return new FromGraphTerm(loopingClause, List.of(matchClause));
    }

    @Override
    protected List<Projection> buildProjectionList(Consumer<VariableExpr> substitutionAdder) {
        Function<VariableExpr, Projection> projectionMapper = v -> {
            substitutionAdder.accept(v);
            VarIdentifier rightIdentifier = SqlppVariableUtil.toUserDefinedVariableName(v.getVar());
            return new Projection(Projection.Kind.NAMED_EXPR, v, rightIdentifier.getValue());
        };

        // We will expose our output variables.
        List<Projection> projectionList = new ArrayList<>();
        loopingClause.getOutputVariables().stream().map(projectionMapper).forEach(projectionList::add);
        return projectionList;
    }

    @Override
    protected void mergeExternalSequences(SelectExpression selectExpression, LoweringEnvironment environment)
            throws CompilationException {
        // We will use a LET-CLAUSE here so our translated FIXED-POINT operator can see its anchor inputs.
        VariableExpr nestingVariableCopy = new VariableExpr(nestingVariable.getVar());
        LetClause letClause = new LetClause(nestingVariableCopy, selectExpression);
        environment.acceptTransformer(clauseSequence -> clauseSequence.addNonRepresentativeClause(letClause));
    }

    @Override
    protected void introduceBindings(LoweringEnvironment environment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        GraphixDeepCopyVisitor expressionDeepCopyVisitor = new GraphixDeepCopyVisitor();
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();
        environment.acceptTransformer(clauseSequence -> {
            // Expose our schema variable to our main sequence...
            VariableExpr schemaJoinAlias = graphixRewritingContext.getGraphixVariableCopy(schemaVariable);
            Expression schemaVariableAccess = (Expression) remapCloneVisitor.substitute(schemaVariable);
            clauseSequence.addNonRepresentativeClause(new LetClause(schemaJoinAlias, schemaVariableAccess));
            aliasLookupTable.addIterationAlias(schemaVariable, schemaJoinAlias);
            aliasLookupTable.addJoinAlias(schemaVariable, schemaJoinAlias);

            // ...and our destination vertex binding...
            VariableExpr destJoinAlias = graphixRewritingContext.getGraphixVariableCopy(destVariable);
            Expression destAccess = (Expression) remapCloneVisitor.substitute(destVariable);
            clauseSequence.addNonRepresentativeClause(new LetClause(destJoinAlias, destAccess));
            aliasLookupTable.addIterationAlias(destVariable, destJoinAlias);
            aliasLookupTable.addJoinAlias(destVariable, destJoinAlias);

            // Ensure that the path we yield has the correct direction.
            FunctionIdentifier translateFunctionID = (evaluationDirection == pathDescriptor.getElementDirection())
                    ? GraphixFunctionIdentifiers.TRANSLATE_FORWARD_PATH
                    : GraphixFunctionIdentifiers.TRANSLATE_REVERSE_PATH;

            // Add our path binding (a deep copy is required here).
            CallExpr translatePathPatternExpr = new CallExpr(new FunctionSignature(translateFunctionID),
                    List.of((Expression) remapCloneVisitor.substitute(pathVariable)));
            VariableExpr descriptorVariable = pathDescriptor.getVariableExpr();
            VariableExpr pathJoinAlias = graphixRewritingContext.getGraphixVariableCopy(descriptorVariable);
            VariableExpr pathVariableCopy = expressionDeepCopyVisitor.visit(descriptorVariable, null);
            clauseSequence.addNonRepresentativeClause(new LetClause(pathJoinAlias, translatePathPatternExpr));
            clauseSequence.addPathBinding(pathVariableCopy, pathJoinAlias);
            aliasLookupTable.addIterationAlias(descriptorVariable, pathJoinAlias);
            aliasLookupTable.addJoinAlias(descriptorVariable, pathJoinAlias);
        });
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        super.apply(environment);

        // Build a two term condition, to handle our minimum hops and maximum hops.
        AbstractExpression minimumHopCountCondition;
        AbstractExpression maximumHopCountCondition;
        if (pathDescriptor.getMinimumHops() == 0) {
            minimumHopCountCondition = new LiteralExpr(TrueLiteral.INSTANCE);

        } else {
            Expression minimumHopExpr = new LiteralExpr(new IntegerLiteral(pathDescriptor.getMinimumHops()));
            VariableExpr pathAccess = environment.getAliasLookupTable().getJoinAlias(pathDescriptor.getVariableExpr());
            Expression countPathHopsExpr = generatePathHopCountExpression(pathAccess);
            List<Expression> conditionArgs = List.of(countPathHopsExpr, minimumHopExpr);
            minimumHopCountCondition = new OperatorExpr(conditionArgs, List.of(OperatorType.GE), false);
        }
        if (pathDescriptor.getMaximumHops() == null) {
            maximumHopCountCondition = new LiteralExpr(TrueLiteral.INSTANCE);

        } else {
            Expression maximumHopExpr = new LiteralExpr(new IntegerLiteral(pathDescriptor.getMaximumHops()));
            VariableExpr pathAccess = environment.getAliasLookupTable().getJoinAlias(pathDescriptor.getVariableExpr());
            Expression countPathHopsExpr = generatePathHopCountExpression(pathAccess);
            List<Expression> conditionArgs = List.of(countPathHopsExpr, maximumHopExpr);
            maximumHopCountCondition = new OperatorExpr(conditionArgs, List.of(OperatorType.LE), false);
        }
        minimumHopCountCondition.setSourceLocation(sourceLocation);
        maximumHopCountCondition.setSourceLocation(sourceLocation);
        OperatorExpr yieldCriterion = new OperatorExpr();
        yieldCriterion.addOperand(minimumHopCountCondition);
        yieldCriterion.addOperand(maximumHopCountCondition);
        yieldCriterion.addOperator(OperatorType.AND);
        yieldCriterion.setSourceLocation(sourceLocation);
        WhereClause hopCountFilter = new WhereClause(yieldCriterion);
        hopCountFilter.setSourceLocation(sourceLocation);
        environment.acceptTransformer(c -> c.addNonRepresentativeClause(hopCountFilter));
    }

    private static class LoopingClauseBuilder {
        private final AnchorSequenceAction anchorSequenceAction;
        private final RecursiveSequenceAction recursiveSequenceAction;

        private VariableExpr outputPathVariable;
        private VariableExpr outputDestVariable;
        private VariableExpr outputSchemaVariable;

        public LoopingClauseBuilder(AnchorSequenceAction anchorAction, RecursiveSequenceAction recursiveAction) {
            this.anchorSequenceAction = anchorAction;
            this.recursiveSequenceAction = recursiveAction;
        }

        public void setOutputPathVariable(VariableExpr outputPathVariable) {
            this.outputPathVariable = outputPathVariable;
        }

        public void setOutputDestVariable(VariableExpr outputDestVariable) {
            this.outputDestVariable = outputDestVariable;
        }

        public void setOutputSchemaVariable(VariableExpr outputSchemaVariable) {
            this.outputSchemaVariable = outputSchemaVariable;
        }

        public LoopingClause build() {
            // Our input list must follow the format below.
            VariableExpr[] inputVariableArray = new VariableExpr[3];
            inputVariableArray[DEST_VARIABLES_INDEX] = anchorSequenceAction.getNextVertexVariables().get(0);
            inputVariableArray[PATH_VARIABLE_INDEX] = anchorSequenceAction.getProducedPathVariables().get(0);
            inputVariableArray[SCHEMA_VARIABLE_INDEX] = anchorSequenceAction.getSchemaVariables().get(0);
            List<VariableExpr> inputVariables = Arrays.asList(inputVariableArray);

            // Our output list must follow the format below.
            VariableExpr[] outputVariableArray = new VariableExpr[3];
            outputVariableArray[DEST_VARIABLES_INDEX] = outputDestVariable;
            outputVariableArray[PATH_VARIABLE_INDEX] = outputPathVariable;
            outputVariableArray[SCHEMA_VARIABLE_INDEX] = outputSchemaVariable;
            List<VariableExpr> outputVariables = Arrays.asList(outputVariableArray);

            // Iterate through our lowered decompositions.
            List<List<VariableExpr>> previousVariables = new ArrayList<>();
            List<List<VariableExpr>> nextVariables = new ArrayList<>();
            List<SequenceClause> clauseList = new ArrayList<>();
            for (int i = 0; i < recursiveSequenceAction.getLoweredDecompositions().size(); i++) {
                ClauseSequence clauseSequence = recursiveSequenceAction.getLoweredDecompositions().get(i);
                VariableExpr previousVertexVariable = recursiveSequenceAction.getInputVertexVariables().get(i);
                VariableExpr previousPathVariable = recursiveSequenceAction.getInputPathVariables().get(i);
                VariableExpr previousSchemaVariable = recursiveSequenceAction.getInputSchemaVariables().get(i);
                VariableExpr nextVertexVariable = recursiveSequenceAction.getNextVertexVariables().get(i);
                VariableExpr nextPathVariable = recursiveSequenceAction.getProducedPathVariables().get(i);
                VariableExpr nextSchemaVariable = recursiveSequenceAction.getSchemaVariables().get(i);
                clauseList.add(new SequenceClause(clauseSequence));

                // Our previous and output lists must follow the format below. Note that we do not forward our edge.
                VariableExpr[] prevVariableArray = new VariableExpr[3];
                VariableExpr[] nextVariableArray = new VariableExpr[3];
                prevVariableArray[DEST_VARIABLES_INDEX] = previousVertexVariable;
                prevVariableArray[PATH_VARIABLE_INDEX] = previousPathVariable;
                prevVariableArray[SCHEMA_VARIABLE_INDEX] = previousSchemaVariable;
                nextVariableArray[DEST_VARIABLES_INDEX] = nextVertexVariable;
                nextVariableArray[PATH_VARIABLE_INDEX] = nextPathVariable;
                nextVariableArray[SCHEMA_VARIABLE_INDEX] = nextSchemaVariable;
                previousVariables.add(Arrays.asList(prevVariableArray));
                nextVariables.add(Arrays.asList(nextVariableArray));
            }

            // Finally, build our looping clause.
            return new LoopingClause(inputVariables, outputVariables, anchorSequenceAction.getAuxiliaryVariables(),
                    previousVariables, nextVariables, clauseList);
        }
    }
}
