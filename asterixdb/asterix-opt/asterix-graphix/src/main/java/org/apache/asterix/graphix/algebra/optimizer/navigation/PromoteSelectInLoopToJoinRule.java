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
package org.apache.asterix.graphix.algebra.optimizer.navigation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.graphix.algebra.annotations.JoinInLoopOperatorAnnotations;
import org.apache.asterix.graphix.algebra.finder.InExpressionFunctionFinder;
import org.apache.asterix.graphix.algebra.finder.ProducerOperatorFinder;
import org.apache.asterix.graphix.algebra.finder.VariableInReductionFinder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Given a plan with a {@link RecursiveTailOperator}, introduce an {@link InnerJoinOperator} to replace a
 * {@link SelectOperator} that contains an expression that has a functional dependency on a previous variable (within
 * the loop, from {@link RecursiveTailOperator}). Additionally, the {@link InnerJoinOperator} should:
 * <ol>
 *   <li>Have a left branch with a {@link RecursiveTailOperator}.</li>
 *   <li>Have a right branch with no {@link RecursiveTailOperator}s and only {@link EmptyTupleSourceOperator}s.</li>
 * </ol>
 */
public class PromoteSelectInLoopToJoinRule implements IAlgebraicRewriteRule {
    private final Deque<Collection<LogicalVariable>> previousVariableQueue = new ArrayDeque<>();

    public final static int JOIN_PERSISTENT_INPUT_INDEX = 1;
    public final static int JOIN_TRANSIENT_INPUT_INDEX = 0;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        switch (opRef.getValue().getOperatorTag()) {
            case DISTRIBUTE_RESULT:
            case DELEGATE_OPERATOR:
            case SINK:
                // We are at our root. Reset our state.
                previousVariableQueue.clear();
                break;

            case RECURSIVE_TAIL:
                // On post-traversal, we should always work with the lowest tail first.
                RecursiveTailOperator rsOp = (RecursiveTailOperator) opRef.getValue();
                previousVariableQueue.addFirst(rsOp.getPreviousIterationVariables());
                break;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator selectOp = (SelectOperator) op;

        // We are looking for a SELECT that contains a (functionally dependent) recursive source variable.
        List<LogicalVariable> usedVariablesInOp = new ArrayList<>();
        List<LogicalVariable> dependentUsedVariables = new ArrayList<>();
        getUsedVariablesInSelect(selectOp, usedVariablesInOp);
        Collection<LogicalVariable> previousVariables = null;
        for (LogicalVariable usedVariable : usedVariablesInOp) {
            ProducerOperatorFinder opFinder = new ProducerOperatorFinder(selectOp);
            if (!opFinder.search(usedVariable)) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Producer operator not found?");
            }
            PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(opFinder.get(), context);
            List<FunctionalDependency> fdList = context.getFDList(opFinder.get());
            VariableInReductionFinder variableInReductionFinder = new VariableInReductionFinder(fdList, usedVariable);
            for (Collection<LogicalVariable> workingPreviousVarSet : previousVariableQueue) {
                if (workingPreviousVarSet.stream().anyMatch(variableInReductionFinder::search)) {
                    if (previousVariables == null) {
                        // TODO (GLENN): How should we handle SELECTs that reduce to variables across multiple loops?
                        previousVariables = workingPreviousVarSet;
                    }
                    dependentUsedVariables.add(usedVariable);
                    break;
                }
            }
        }
        if (previousVariables == null || dependentUsedVariables.size() == usedVariablesInOp.size()) {
            // We have not found any dependent variables, OR all variables are dependent.
            return false;
        }

        // We have found an appropriate SELECT.
        Deque<ILogicalOperator> operatorStack = new ArrayDeque<>(List.of(selectOp));
        Deque<ILogicalOperator> visitedStack = new ArrayDeque<>(List.of(selectOp));
        while (!operatorStack.isEmpty()) {
            ILogicalOperator workingOp = operatorStack.removeLast();
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                ILogicalOperator inputOp = inputRef.getValue();
                if (inputOp.getOperatorTag() != LogicalOperatorTag.RECURSIVE_TAIL) {
                    operatorStack.addLast(inputOp);
                    visitedStack.addLast(inputOp);

                } else {
                    // If we have already introduced a JOIN for this loop, exit here.
                    if (context.checkIfInDontApplySet(this, inputOp)) {
                        return false;
                    }

                    // We have found our recursive tail operator (there should only be one here). Move upward.
                    ILogicalOperator downstreamOp = visitedStack.removeLast();
                    while (downstreamOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
                        downstreamOp = visitedStack.removeLast();
                    }
                    DataSourceScanOperator dataSourceScanOp = (DataSourceScanOperator) downstreamOp;

                    // If this SELECT does not (partially) reduce to our SCAN, exit here.
                    if (!doesSelectReduceToScan(selectOp, dataSourceScanOp, context)) {
                        return false;
                    }

                    // Transform this SELECT into an INNER-JOIN operator.
                    InnerJoinOperator joinOp = new InnerJoinOperator(selectOp.getCondition());
                    joinOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                    joinOp.setSourceLocation(selectOp.getSourceLocation());
                    JoinInLoopOperatorAnnotations.markAsJoinInLoop(joinOp);
                    opRef.setValue(joinOp);

                    // Partition our plan into a persistent (non-recursive) part...
                    List<ILogicalOperator> persistentOps = getPersistentOps(selectOp, dataSourceScanOp, context);
                    for (int i = 0; i < persistentOps.size() - 1; i++) {
                        ILogicalOperator parentOp = persistentOps.get(i);
                        ILogicalOperator childOp = persistentOps.get(i + 1);
                        parentOp.getInputs().clear();
                        parentOp.getInputs().add(new MutableObject<>(childOp));
                    }
                    ILogicalOperator persistentInputOp = persistentOps.get(0);

                    // ...and a transient (recursive) part.
                    List<ILogicalOperator> transientOps = getTransientOps(selectOp, persistentOps);
                    for (int i = 0; i < transientOps.size() - 1; i++) {
                        ILogicalOperator parentOp = transientOps.get(i);
                        ILogicalOperator childOp = transientOps.get(i + 1);
                        parentOp.getInputs().clear();
                        parentOp.getInputs().add(new MutableObject<>(childOp));
                    }
                    ILogicalOperator transientInputOp = transientOps.get(0);

                    // Connect each input to our JOIN.
                    joinOp.getInputs().addAll(List.of(new MutableObject<>(), new MutableObject<>()));
                    joinOp.getInputs().get(JOIN_TRANSIENT_INPUT_INDEX).setValue(transientInputOp);
                    joinOp.getInputs().get(JOIN_PERSISTENT_INPUT_INDEX).setValue(persistentInputOp);
                    EmptyTupleSourceOperator etsOp = new EmptyTupleSourceOperator();
                    etsOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                    etsOp.setSourceLocation(selectOp.getSourceLocation());
                    dataSourceScanOp.getInputs().clear();
                    dataSourceScanOp.getInputs().add(new MutableObject<>(etsOp));
                    OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
                    context.addToDontApplySet(this, inputOp);
                    previousVariableQueue.remove(previousVariables);
                    return true;
                }
            }
        }
        return false;
    }

    private void getUsedVariablesInSelect(SelectOperator selectOp, List<LogicalVariable> usedVariables) {
        Mutable<ILogicalExpression> selectCondExprRef = selectOp.getCondition();
        ILogicalExpression selectCondExpr = selectCondExprRef.getValue();
        List<Mutable<ILogicalExpression>> selectCondConjuncts = new ArrayList<>();
        if (selectCondExpr.splitIntoConjuncts(selectCondConjuncts)) {
            for (Mutable<ILogicalExpression> conjunctExprRef : selectCondConjuncts) {
                // We are excluding cycle-constraint functions here.
                if (!new InExpressionFunctionFinder(GraphixFunctionIdentifiers.IS_DISTINCT_).search(conjunctExprRef)) {
                    conjunctExprRef.getValue().getUsedVariables(usedVariables);
                }
            }

        } else if (!new InExpressionFunctionFinder(GraphixFunctionIdentifiers.IS_DISTINCT_).search(selectCondExprRef)) {
            selectOp.getCondition().getValue().getUsedVariables(usedVariables);
        }
    }

    private boolean doesSelectReduceToScan(ILogicalOperator workingOp, DataSourceScanOperator scanOp,
            IOptimizationContext context) throws AlgebricksException {
        Set<LogicalVariable> usedVariables = new HashSet<>();
        VariableUtilities.getUsedVariables(workingOp, usedVariables);
        for (LogicalVariable usedVariable : usedVariables) {
            AtomicBoolean doesUsedReduce = new AtomicBoolean(false);
            new ProducerOperatorFinder(workingOp).searchAndUse(usedVariable, op -> {
                PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
                VariableInReductionFinder variableInReductionFinder =
                        new VariableInReductionFinder(context.getFDList(op), usedVariable);
                if (scanOp.getScanVariables().stream().anyMatch(variableInReductionFinder::search)) {
                    doesUsedReduce.set(true);
                }
            }, () -> new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Producer op not found?"));
            if (doesUsedReduce.get()) {
                return true;
            }
        }
        return false;
    }

    private List<ILogicalOperator> getPersistentOps(ILogicalOperator selectOp, DataSourceScanOperator scanOp,
            IOptimizationContext context) throws AlgebricksException {
        List<ILogicalOperator> persistentOperators = new ArrayList<>();

        // Everything from our SELECT to our DATASOURCESCAN are **candidates** for our persistent operator set.
        ILogicalOperator belowSelectOp = selectOp.getInputs().get(0).getValue();
        while (!belowSelectOp.equals(scanOp)) {
            persistentOperators.add(belowSelectOp);
            if (belowSelectOp.getInputs().size() > 1) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Unexpected multi-input operator encountered!");
            }
            belowSelectOp = belowSelectOp.getInputs().get(0).getValue();
        }

        // Now remove all operators that don't use variables directly from our DATASOURCESCAN and aren't ASSIGNs.
        Iterator<ILogicalOperator> opIterator = persistentOperators.iterator();
        while (opIterator.hasNext()) {
            ILogicalOperator persistentOp = opIterator.next();
            if (persistentOp.getOperatorTag() != LogicalOperatorTag.ASSIGN
                    || !doesSelectReduceToScan(persistentOp, scanOp, context)) {
                opIterator.remove();
            }
        }

        // Our SCAN op should always be the last operator here.
        persistentOperators.add(scanOp);
        return persistentOperators;
    }

    private List<ILogicalOperator> getTransientOps(SelectOperator selectOp, List<ILogicalOperator> persistentOps)
            throws AlgebricksException {
        // Everything below our SELECT operator that is not already marked as persistent belongs to the transient ops.
        List<ILogicalOperator> transientOps = new ArrayList<>();
        ILogicalOperator belowSelectOp = selectOp.getInputs().get(0).getValue();
        while (belowSelectOp.hasInputs()) {
            if (!persistentOps.contains(belowSelectOp)) {
                transientOps.add(belowSelectOp);
            }
            if (belowSelectOp.getInputs().size() > 1) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Unexpected multi-input operator encountered!");
            }
            belowSelectOp = belowSelectOp.getInputs().get(0).getValue();
        }
        transientOps.add(belowSelectOp);
        return transientOps;
    }
}
