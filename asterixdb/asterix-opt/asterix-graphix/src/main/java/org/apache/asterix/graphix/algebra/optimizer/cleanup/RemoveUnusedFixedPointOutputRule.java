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
package org.apache.asterix.graphix.algebra.optimizer.cleanup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.annotations.FixedPointOperatorAnnotations;
import org.apache.asterix.graphix.algebra.variable.LoopVariableContext;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveHeadOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Remove any output variables of a {@link FixedPointOperator} and the input of a {@link RecursiveTailOperator}
 * <i>iff</i> the variable is not used within the loop and downstream --- for now, we restrict this removal to the
 * schema variable.
 */
public class RemoveUnusedFixedPointOutputRule implements IAlgebraicRewriteRule {
    private final Deque<ILogicalOperator> upstreamOpStack = new ArrayDeque<>();
    private final List<RemovalContext> removalContextList = new ArrayList<>();

    private final static class RemovalContext {
        private final Set<LogicalVariable> markedVariables = new LinkedHashSet<>();

        private final FixedPointOperator fpOp;
        private final RecursiveTailOperator rtOp;
        private final RecursiveHeadOperator rhOp;

        private RemovalContext(FixedPointOperator fpOp, RecursiveTailOperator rtOp, RecursiveHeadOperator rhOp) {
            this.fpOp = fpOp;
            this.rtOp = rtOp;
            this.rhOp = rhOp;
        }

        public boolean isVariableContained(LogicalVariable v) {
            for (LogicalVariable outputVariable : fpOp.getOutputVariableList()) {
                boolean isKeyMatch = outputVariable == v;
                boolean isAnchorMatch = fpOp.getInvertedAnchorOutputMap().get(outputVariable) == v;
                boolean isRecursiveMatch = fpOp.getInvertedRecursiveOutputMap().get(outputVariable) == v;
                if (isKeyMatch || isAnchorMatch || isRecursiveMatch) {
                    return true;
                }
            }
            return false;
        }

        public void markVariableAsUsed(LogicalVariable v) {
            markedVariables.add(v);
        }

        public Set<LogicalVariable> getUnusedOutputVariables() {
            Set<LogicalVariable> markedOutputVariables = markedVariables.stream().map(v -> {
                if (fpOp.getAnchorOutputMap().containsKey(v)) {
                    return fpOp.getAnchorOutputMap().get(v);

                } else if (fpOp.getRecursiveOutputMap().containsKey(v)) {
                    return fpOp.getRecursiveOutputMap().get(v);
                }
                return v;
            }).collect(Collectors.toSet());
            Set<LogicalVariable> allOutputVariables = new LinkedHashSet<>(fpOp.getOutputVariableList());
            allOutputVariables.removeAll(markedOutputVariables);

            // TODO (GLENN): Expand this to include more than just the schema variable.
            LoopVariableContext loopVariableContext = FixedPointOperatorAnnotations.getLoopVariableContext(fpOp);
            LogicalVariable schemaOutputVariable = loopVariableContext.getSchemaVariable();
            if (schemaOutputVariable == null) {
                return Collections.emptySet();
            }
            LogicalVariable anchorOutputVariable = fpOp.getInvertedAnchorOutputMap().get(schemaOutputVariable);
            LogicalVariable recursiveOutputVariable = fpOp.getInvertedRecursiveOutputMap().get(schemaOutputVariable);
            allOutputVariables.removeIf(
                    v -> v != schemaOutputVariable && v != anchorOutputVariable && v != recursiveOutputVariable);
            return allOutputVariables;
        }
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        switch (op.getOperatorTag()) {
            case DISTRIBUTE_RESULT:
            case DELEGATE_OPERATOR:
            case SINK:
                // We are at our root. Reset our state.
                upstreamOpStack.clear();
                removalContextList.clear();
                upstreamOpStack.addLast(op);
                return false;

            case RECURSIVE_TAIL:
                upstreamOpStack.addLast(op);
                break;

            default:
                upstreamOpStack.addLast(op);
                return false;
        }
        RecursiveTailOperator rtOp = (RecursiveTailOperator) op;
        FixedPointOperator fpOp = rtOp.getFixedPointOp();
        RecursiveHeadOperator rhOp = null;

        // Walk back up our operator stack to find our fixed-point operator, then our recursive head.
        boolean hasEncounteredFixedPointOp = false;
        boolean hasEncounteredRecursiveHeadOp = false;
        for (Iterator<ILogicalOperator> it = upstreamOpStack.descendingIterator(); it.hasNext();) {
            ILogicalOperator upstreamOp = it.next();

            // Note: we do not allow nested-recursion, so this is mainly a sanity check.
            switch (upstreamOp.getOperatorTag()) {
                case FIXED_POINT:
                    hasEncounteredFixedPointOp = true;
                    break;

                case RECURSIVE_HEAD:
                    if (!hasEncounteredFixedPointOp) {
                        throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                                "Nested recursive calls are not allowed!");
                    }
                    hasEncounteredRecursiveHeadOp = true;
                    rhOp = (RecursiveHeadOperator) upstreamOp;
                    break;

                default:
                    break;
            }
            if (hasEncounteredRecursiveHeadOp) {
                break;
            }
        }

        // Add this triple to our context list.
        removalContextList.add(new RemovalContext(fpOp, rtOp, rhOp));
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        upstreamOpStack.removeLast();
        switch (op.getOperatorTag()) {
            case FIXED_POINT:
            case RECURSIVE_HEAD:
                return false;
        }

        // Does this operator use any FP variables? If so, then remove the FP operator from consideration.
        List<LogicalVariable> usedVariables = new ArrayList<>();
        VariableUtilities.getUsedVariables(op, usedVariables);
        for (LogicalVariable usedVariable : usedVariables) {
            removalContextList.stream().filter(c -> c.isVariableContained(usedVariable)).findFirst()
                    .ifPresent(c -> c.markVariableAsUsed(usedVariable));
        }

        // We have walked back up to the root operator. Find all FP/RT/RS operators with unused variables.
        if (upstreamOpStack.isEmpty()) {
            boolean isModified = false;
            for (RemovalContext removalContext : removalContextList) {
                for (LogicalVariable unusedVariable : removalContext.getUnusedOutputVariables()) {
                    // We have found an unused variable. Locate the recursive & anchor pair.
                    FixedPointOperator fpOp = removalContext.fpOp;
                    LogicalVariable anchorVariable = fpOp.getInvertedAnchorOutputMap().get(unusedVariable);
                    LogicalVariable recursiveVariable = fpOp.getInvertedRecursiveOutputMap().get(unusedVariable);

                    // Find the producer of our anchor variable, and remove it.
                    Deque<Mutable<ILogicalOperator>> inputOpRefStack = new ArrayDeque<>();
                    inputOpRefStack.add(fpOp.getInputs().get(0));
                    while (!inputOpRefStack.isEmpty()) {
                        Mutable<ILogicalOperator> inputOpRef = inputOpRefStack.removeLast();
                        List<LogicalVariable> producedVariables = new ArrayList<>();
                        VariableUtilities.getProducedVariables(inputOpRef.getValue(), producedVariables);
                        if (producedVariables.contains(anchorVariable)) {
                            if (inputOpRef.getValue().getInputs().size() != 1) {
                                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                                        "Unexpected multi-input-operator encountered!");
                            }
                            inputOpRef.setValue(inputOpRef.getValue().getInputs().get(0).getValue());
                            break;
                        }
                        inputOpRef.getValue().getInputs().forEach(inputOpRefStack::addLast);
                    }

                    // Find the producer of our recursive variable, and remove it.
                    inputOpRefStack.clear();
                    inputOpRefStack.add(fpOp.getNestedPlans().get(0).getRoots().get(0));
                    while (!inputOpRefStack.isEmpty()) {
                        Mutable<ILogicalOperator> inputOpRef = inputOpRefStack.removeLast();
                        List<LogicalVariable> producedVariables = new ArrayList<>();
                        VariableUtilities.getProducedVariables(inputOpRef.getValue(), producedVariables);
                        if (producedVariables.contains(recursiveVariable)) {
                            if (inputOpRef.getValue().getInputs().size() != 1) {
                                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                                        "Unexpected multi-input-operator encountered!");
                            }
                            inputOpRef.setValue(inputOpRef.getValue().getInputs().get(0).getValue());
                            break;
                        }
                        inputOpRef.getValue().getInputs().forEach(inputOpRefStack::addLast);
                    }

                    // Remove the variable from fixed-point output and head-output.
                    RecursiveHeadOperator rhOp = removalContext.rhOp;
                    fpOp.replaceOutputMaps(copyMapWithoutVariable(fpOp.getAnchorOutputMap(), unusedVariable),
                            copyMapWithoutVariable(fpOp.getRecursiveOutputMap(), unusedVariable));
                    rhOp.replaceOutputMap(copyMapWithoutVariable(rhOp.getRecursiveOutputMap(), unusedVariable));
                    LoopVariableContext loopContext = FixedPointOperatorAnnotations.getLoopVariableContext(fpOp);
                    if (unusedVariable.equals(loopContext.getPathVariable())) {
                        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp,
                                new LoopVariableContext(null, loopContext.getSchemaVariable(),
                                        loopContext.getEndingVariables(), loopContext.getStartingVariables(),
                                        loopContext.getDownVariables()));

                    } else if (unusedVariable.equals(loopContext.getSchemaVariable())) {
                        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp,
                                new LoopVariableContext(loopContext.getPathVariable(), null,
                                        loopContext.getEndingVariables(), loopContext.getStartingVariables(),
                                        loopContext.getDownVariables()));

                    } else if (loopContext.getEndingVariables().contains(unusedVariable)) {
                        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp,
                                new LoopVariableContext(loopContext.getPathVariable(), loopContext.getSchemaVariable(),
                                        loopContext.getEndingVariables().stream().filter(v -> !unusedVariable.equals(v))
                                                .collect(Collectors.toList()),
                                        loopContext.getStartingVariables(), loopContext.getDownVariables()));

                    } else if (loopContext.getStartingVariables().contains(unusedVariable)) {
                        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp,
                                new LoopVariableContext(loopContext.getPathVariable(), loopContext.getSchemaVariable(),
                                        loopContext.getEndingVariables(),
                                        loopContext.getStartingVariables().stream()
                                                .filter(v -> !unusedVariable.equals(v)).collect(Collectors.toList()),
                                        loopContext.getDownVariables()));

                    } else if (loopContext.getDownVariables().contains(unusedVariable)) {
                        FixedPointOperatorAnnotations.setLoopVariableContext(fpOp,
                                new LoopVariableContext(loopContext.getPathVariable(), loopContext.getSchemaVariable(),
                                        loopContext.getEndingVariables(), loopContext.getStartingVariables(),
                                        loopContext.getDownVariables().stream().filter(v -> !unusedVariable.equals(v))
                                                .collect(Collectors.toList())));
                    }

                    // For our tail input, we need to first find the mapped-to variable on our anchor map.
                    RecursiveTailOperator rtOp = removalContext.rtOp;
                    LogicalVariable anchorInputUnusedVariable = rtOp.getRecursiveInputMap().get(unusedVariable);
                    rtOp.replaceInputMaps(copyMapWithoutVariable(rtOp.getAnchorInputMap(), anchorInputUnusedVariable),
                            copyMapWithoutVariable(rtOp.getRecursiveInputMap(), unusedVariable));
                    isModified = true;
                }
            }
            if (isModified) {
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(op, context);
                return true;
            }
        }
        return false;
    }

    private <T> Map<LogicalVariable, T> copyMapWithoutVariable(Map<LogicalVariable, T> oldMap, LogicalVariable v) {
        return oldMap.entrySet().stream().filter(e -> e.getKey() != v && e.getValue() != v)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
