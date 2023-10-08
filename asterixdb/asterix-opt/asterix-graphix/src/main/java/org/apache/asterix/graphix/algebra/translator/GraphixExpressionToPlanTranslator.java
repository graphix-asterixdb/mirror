/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.translator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.annotations.FixedPointOperatorAnnotations;
import org.apache.asterix.graphix.algebra.variable.LoopVariableContext;
import org.apache.asterix.graphix.algebra.variable.LoopVariableCopyCallback;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.clause.extension.BranchingClauseExtension;
import org.apache.asterix.graphix.lang.clause.extension.FromGraphClauseExtension;
import org.apache.asterix.graphix.lang.clause.extension.IGraphixVisitorExtension;
import org.apache.asterix.graphix.lang.clause.extension.LoopingClauseExtension;
import org.apache.asterix.graphix.lang.clause.extension.SequenceClauseExtension;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MarkerSinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveHeadOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;

public class GraphixExpressionToPlanTranslator extends SqlppExpressionToPlanTranslator {
    private final Map<String, List<Integer>> nameToIdMap = new LinkedHashMap<>();
    private final Function<VariableExpr, LogicalVariable> variableGetter = v -> {
        if (context.getVar(v.getVar().getId()) == null) {
            // Our variable must have been remapped sometime during the SQL++ rewrites.
            for (Integer otherId : nameToIdMap.get(v.getVar().getValue())) {
                if (context.getVar(otherId) != null) {
                    return context.getVar(otherId);
                }
            }
            throw new IllegalStateException("Variable not found?");
        }
        return context.getVar(v.getVar().getId());
    };

    // For upstream operators to use variables that are defined downstream.
    private final Map<VariableExpr, Consumer<LogicalVariable>> downstreamCallbackMap = new HashMap<>();

    public GraphixExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounter,
            Map<VarIdentifier, IAObject> externalVars) throws AlgebricksException {
        super(metadataProvider, currentVarCounter, externalVars);
    }

    // To properly handle SELECT *, we'll override this method.
    @Override
    protected void getFromBindings(FromClause fromClause, List<FieldBinding> outFieldBindings,
            Set<String> outFieldNames, Predicate<VariableExpr> varTest) throws CompilationException {
        super.getFromBindings(fromClause, outFieldBindings, outFieldNames, varTest);
        if (!(fromClause instanceof FromGraphClause)) {
            return;
        }
        FromGraphClause fromGraphClause = (FromGraphClause) fromClause;
        List<VariableExpr> fromGraphTermVariables = fromGraphClause.getFromGraphTerms().stream()
                .map(FromGraphTerm::getMatchClauses).flatMap(Collection::stream)
                .map(m -> IteratorUtils.toList(m.iterator())).flatMap(Collection::stream)
                .map(q -> IteratorUtils.toList(q.iterator())).flatMap(Collection::stream).map(c -> {
                    switch (c.getPatternType()) {
                        case VERTEX_PATTERN:
                            VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) c;
                            VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                            return vertexDescriptor.getVariableExpr();
                        case EDGE_PATTERN:
                            EdgePatternExpr edgePatternExpr = (EdgePatternExpr) c;
                            EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                            return edgeDescriptor.getVariableExpr();
                        case PATH_PATTERN:
                            PathPatternExpr pathPatternExpr = (PathPatternExpr) c;
                            PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
                            return pathDescriptor.getVariableExpr();
                    }
                    throw new IllegalStateException();
                }).collect(Collectors.toList());
        for (VariableExpr fromGraphTermVariable : fromGraphTermVariables) {
            if (varTest != null && varTest.test(fromGraphTermVariable)) {
                outFieldBindings.add(getFieldBinding(fromGraphTermVariable, outFieldNames));
            } else {
                outFieldBindings.add(getFieldBinding(fromGraphTermVariable, outFieldNames));
            }
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IVisitorExtension ve, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        if (ve instanceof IGraphixVisitorExtension) {
            IGraphixVisitorExtension gve = (IGraphixVisitorExtension) ve;
            switch (gve.getKind()) {
                case FROM_GRAPH_CLAUSE:
                    FromGraphClauseExtension fgce = (FromGraphClauseExtension) gve;
                    return translateFromGraphClause(fgce, tupSource);

                case SEQUENCE_CLAUSE:
                    SequenceClauseExtension sce = (SequenceClauseExtension) gve;
                    return translateSequenceClause(sce, tupSource);

                case BRANCHING_CLAUSE:
                    BranchingClauseExtension bce = (BranchingClauseExtension) gve;
                    return translateBranchingClause(bce, tupSource);

                case LOOPING_CLAUSE:
                    LoopingClauseExtension lce = (LoopingClauseExtension) gve;
                    return translateLoopingClause(lce, tupSource);

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            "Encountered illegal type of Graphix visitor extension!");
            }
        }
        return super.visit(ve, tupSource);
    }

    public Pair<ILogicalOperator, LogicalVariable> translateFromGraphClause(FromGraphClauseExtension fgce,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        Mutable<ILogicalOperator> inputSrc = tupSource;
        Pair<ILogicalOperator, LogicalVariable> topUnnest = null;
        for (AbstractClause fromTerm : fgce.getFromGraphClause().getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                topUnnest = visit(fromGraphTerm.getLowerClause().getVisitorExtension(), inputSrc);

            } else {
                topUnnest = fromTerm.accept(this, inputSrc);
            }
            inputSrc = new MutableObject<>(topUnnest.first);
        }
        return topUnnest;
    }

    public Pair<ILogicalOperator, LogicalVariable> translateLoopingClause(LoopingClauseExtension lce,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        LoopingClause loopingClause = lce.getLoopingClause();
        if (loopingClause.getSequenceClauses().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, loopingClause.getSourceLocation(),
                    "Encountered complex (> 1 edge and node) looping clause!");
        }
        SequenceClause sequenceClause = loopingClause.getSequenceClauses().get(0);
        List<LogicalVariable> inputVariables =
                loopingClause.getInputVariables().stream().map(variableGetter).collect(Collectors.toList());

        // Translate our sequence. We introduce our previous variables here to make our plan translator happy. :-)
        SequenceClauseExtension clauseExtension = (SequenceClauseExtension) sequenceClause.getVisitorExtension();
        loopingClause.getPreviousVariables().get(0).forEach(context::newVarFromExpression);
        Mutable<ILogicalOperator> sourceOpRef = new MutableObject<>(new EmptyTupleSourceOperator());
        Pair<ILogicalOperator, LogicalVariable> eo = translateSequenceClause(clauseExtension, sourceOpRef);
        List<LogicalVariable> previousVariables =
                loopingClause.getPreviousVariables().get(0).stream().map(variableGetter).collect(Collectors.toList());
        List<LogicalVariable> nextVariables =
                loopingClause.getNextVariables().get(0).stream().map(variableGetter).collect(Collectors.toList());

        // We will now assemble our loop. We start with our tail (source).
        List<LogicalVariable> outputVariables = loopingClause.getOutputVariables().stream()
                .map(context::newVarFromExpression).collect(Collectors.toList());
        Map<LogicalVariable, LogicalVariable> anchorInputMap = new LinkedHashMap<>();
        Map<LogicalVariable, LogicalVariable> recursiveInputMap = new LinkedHashMap<>();
        for (int i = 0; i < previousVariables.size(); i++) {
            LogicalVariable previousVariable = previousVariables.get(i);
            LogicalVariable anchorVariable = inputVariables.get(i);
            LogicalVariable outputVariable = outputVariables.get(i);
            anchorInputMap.put(anchorVariable, previousVariable);
            recursiveInputMap.put(outputVariable, previousVariable);
        }
        RecursiveTailOperator tailOp = new RecursiveTailOperator(anchorInputMap, recursiveInputMap);
        tailOp.setSourceLocation(loopingClause.getSourceLocation());
        sourceOpRef.setValue(tailOp);

        // Wrap our sequence in a FIXED-POINT operator.
        Iterator<LogicalVariable> anchorOutput = inputVariables.listIterator();
        Iterator<LogicalVariable> recursiveOutput = nextVariables.listIterator();
        Iterator<LogicalVariable> loopOutput = outputVariables.listIterator();
        Iterator<LogicalVariable> previousInput = previousVariables.listIterator();
        Map<LogicalVariable, LogicalVariable> anchorOutputMap = new LinkedHashMap<>();
        Map<LogicalVariable, LogicalVariable> recursiveOutputMap = new LinkedHashMap<>();
        Map<LogicalVariable, LogicalVariable> headOutputMap = new LinkedHashMap<>();
        while (anchorOutput.hasNext() && recursiveOutput.hasNext() && loopOutput.hasNext() && previousInput.hasNext()) {
            LogicalVariable anchorVariable = anchorOutput.next();
            LogicalVariable recursiveVariable = recursiveOutput.next();
            LogicalVariable outputVariable = loopOutput.next();
            anchorOutputMap.put(anchorVariable, outputVariable);
            recursiveOutputMap.put(recursiveVariable, outputVariable);
            headOutputMap.put(outputVariable, previousInput.next());
        }
        List<ILogicalPlan> plansInLoop = List.of(new ALogicalPlanImpl(new MutableObject<>(eo.first)));
        FixedPointOperator fpOp = new FixedPointOperator(anchorOutputMap, recursiveOutputMap, plansInLoop);
        fpOp.setSourceLocation(loopingClause.getSourceLocation());
        fpOp.getInputs().add(tupSource);

        // Annotate our FIXED-POINT operator with its path, schema, and dest variables (to help with our rewrites).
        List<LogicalVariable> auxiliaryVariables =
                loopingClause.getAuxiliaryVariables().stream().map(variableGetter).collect(Collectors.toList());
        if (auxiliaryVariables.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, loopingClause.getSourceLocation(),
                    "Unexpected auxiliary variable encountered in the looping clause!");
        }
        LoopVariableContext loopVariableContext = new LoopVariableContext(fpOp, auxiliaryVariables.get(0));
        downstreamCallbackMap.put(loopingClause.getDownVariables().get(0), loopVariableContext::addDownVariable);
        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp, loopVariableContext);
        fpOp.setOnIsomorphicCopyCallback(new LoopVariableCopyCallback());

        // Build and attach our head operator.
        RecursiveHeadOperator headOp = new RecursiveHeadOperator(headOutputMap);
        headOp.setSourceLocation(loopingClause.getSourceLocation());
        headOp.getInputs().add(new MutableObject<>(fpOp));
        RecursiveTailOperator.updateOperatorReferences(headOp);

        // Add our marker sink operator.
        MarkerSinkOperator markerSinkOp = new MarkerSinkOperator();
        markerSinkOp.setSourceLocation(loopingClause.getSourceLocation());
        markerSinkOp.getInputs().add(new MutableObject<>(headOp));
        return new Pair<>(markerSinkOp, null);
    }

    public Pair<ILogicalOperator, LogicalVariable> translateBranchingClause(BranchingClauseExtension bce,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        BranchingClause branchingClause = bce.getBranchingClause();
        if (branchingClause.getClauseBranches().size() != 2) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, branchingClause.getSourceLocation(),
                    "Encountered non-binary branching-clause!");
        }

        // REPLICATE our bottom operator.
        ReplicateOperator bottomOp = new ReplicateOperator(2);
        bottomOp.getInputs().add(tupSource);

        // Translate our left branch...
        SequenceClause leftBranchClause = branchingClause.getClauseBranches().get(0);
        SequenceClauseExtension leftExtension = (SequenceClauseExtension) leftBranchClause.getVisitorExtension();
        Mutable<ILogicalOperator> leftReplicateOpRef = new MutableObject<>(bottomOp);
        Pair<ILogicalOperator, LogicalVariable> lop = translateSequenceClause(leftExtension, leftReplicateOpRef);

        // ...and our right branch.
        SequenceClause rightBranchClause = branchingClause.getClauseBranches().get(1);
        SequenceClauseExtension rightExtension = (SequenceClauseExtension) rightBranchClause.getVisitorExtension();
        Mutable<ILogicalOperator> rightReplicateOpRef = new MutableObject<>(bottomOp);
        Pair<ILogicalOperator, LogicalVariable> rop = translateSequenceClause(rightExtension, rightReplicateOpRef);

        // Build the variable mapping from each branch.
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> variableMapping =
                branchingClause.getProjectionMapping().entrySet().stream().map(e -> {
                    VariableExpr leftVarExpr = e.getValue().get(0);
                    VariableExpr rightVarExpr = e.getValue().get(1);
                    LogicalVariable leftVar = context.getVar(leftVarExpr.getVar().getId());
                    LogicalVariable rightVar = context.getVar(rightVarExpr.getVar().getId());
                    LogicalVariable outputVar = context.newVar();
                    context.setVar(e.getKey(), outputVar);
                    return new Triple<>(leftVar, rightVar, outputVar);
                }).collect(Collectors.toCollection(ArrayList::new));

        // Finally, UNION-ALL both branches.
        UnionAllOperator unionAllOp = new UnionAllOperator(variableMapping);
        unionAllOp.getInputs().add(new MutableObject<>(lop.first));
        unionAllOp.getInputs().add(new MutableObject<>(rop.first));
        return new Pair<>(unionAllOp, null);
    }

    public Pair<ILogicalOperator, LogicalVariable> translateSequenceClause(SequenceClauseExtension sce,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        ClauseSequence clauseSequence = sce.getSequenceClause().getClauseSequence();
        Iterator<AbstractClause> clauseIterator = clauseSequence.iterator();
        if (!clauseIterator.hasNext()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, clauseSequence.getSourceLocation(),
                    "Encountered empty sequence-clause collection!");
        }
        Mutable<ILogicalOperator> topOpRef = new MutableObject<>();

        // Our first JOIN clause is functionally equivalent to the left expression of a FROM-TERM. Ignore our condition.
        AbstractClause workingClause = clauseIterator.next();
        if (!(workingClause instanceof AbstractBinaryCorrelateClause)) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "First clause of a SequenceClause should be a JOIN!");
        }
        AbstractBinaryCorrelateClause firstJoinClause = (AbstractBinaryCorrelateClause) workingClause;
        LogicalVariable leftVar = context.newVarFromExpression(firstJoinClause.getRightVariable());
        Expression leftExpr = firstJoinClause.getRightExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(leftExpr, tupSource);
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> unnest = makeUnnestExpression(eo.first, eo.second);
        UnnestOperator unnestOp = new UnnestOperator(leftVar, new MutableObject<>(unnest.first));
        unnestOp.getInputs().add(unnest.second);
        topOpRef.setValue(unnestOp);

        // Iterate through the rest of our clauses.
        while (clauseIterator.hasNext()) {
            workingClause = clauseIterator.next();

            // TODO (GLENN): This is a bit hacky... Fix the SQL++ rewrites that potentially messed with our IDs.
            if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                LetClause workingLetClause = (LetClause) workingClause;
                String varName = workingLetClause.getVarExpr().getVar().getValue();
                Pair<ILogicalOperator, LogicalVariable> e0 = workingLetClause.accept(this, topOpRef);
                nameToIdMap.putIfAbsent(varName, new ArrayList<>());
                nameToIdMap.get(varName).add(e0.second.getId());
                topOpRef = new MutableObject<>(e0.first);
                continue;
            }
            topOpRef = new MutableObject<>(workingClause.accept(this, topOpRef).first);

            // Check our callback map.
            Iterator<Map.Entry<VariableExpr, Consumer<LogicalVariable>>> callbackIterator =
                    downstreamCallbackMap.entrySet().iterator();
            while (callbackIterator.hasNext()) {
                Map.Entry<VariableExpr, Consumer<LogicalVariable>> mapEntry = callbackIterator.next();
                int downstreamVariableId = mapEntry.getKey().getVar().getId();
                if (context.getVar(downstreamVariableId) != null) {
                    mapEntry.getValue().accept(context.getVar(downstreamVariableId));
                    callbackIterator.remove();
                }
            }
        }
        return new Pair<>(topOpRef.getValue(), Objects.requireNonNull(leftVar));
    }
}
