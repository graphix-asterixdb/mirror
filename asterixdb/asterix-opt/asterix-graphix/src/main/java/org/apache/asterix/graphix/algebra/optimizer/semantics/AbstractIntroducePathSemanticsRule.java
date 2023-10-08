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
package org.apache.asterix.graphix.algebra.optimizer.semantics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.annotations.FixedPointOperatorAnnotations;
import org.apache.asterix.graphix.algebra.finder.PrimaryKeyVariableFinder;
import org.apache.asterix.graphix.algebra.finder.ProducerOperatorFinder;
import org.apache.asterix.graphix.algebra.finder.VariableInReductionFinder;
import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.graphix.algebra.variable.LoopVariableContext;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;

public abstract class AbstractIntroducePathSemanticsRule implements IAlgebraicRewriteRule {
    protected final Set<FixedPointOperatorContext> contextSet = new LinkedHashSet<>();

    protected static final class FixedPointOperatorContext {
        final Mutable<ILogicalOperator> fpOpRef;
        final List<LogicalVariable> startingVariables;
        final List<LogicalVariable> endingVariables;
        final List<LogicalVariable> downVariables;
        final LogicalVariable pathVariable;

        private FixedPointOperatorContext(Mutable<ILogicalOperator> fpOpRef, List<LogicalVariable> startVariables,
                List<LogicalVariable> endVariables, List<LogicalVariable> downVariables, LogicalVariable pathVariable) {
            this.startingVariables = startVariables;
            this.endingVariables = endVariables;
            this.downVariables = downVariables;
            this.pathVariable = pathVariable;
            this.fpOpRef = fpOpRef;
        }
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        switch (opRef.getValue().getOperatorTag()) {
            case FIXED_POINT:
                // We'll grab our variables from our annotations.
                FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
                LoopVariableContext loopContext = FixedPointOperatorAnnotations.getLoopVariableContext(fpOp);
                LogicalVariable pathVariable = loopContext.getPathVariable();
                List<LogicalVariable> destVariables = loopContext.getEndingVariables();
                List<LogicalVariable> sourceVariables = loopContext.getStartingVariables();
                List<LogicalVariable> downVariables = loopContext.getDownVariables();

                // If the root of our nested-plan is already a semantics operator, then skip this FP...
                List<Mutable<ILogicalOperator>> nestedPlanRoots = fpOp.getNestedPlans().get(0).getRoots();
                if (nestedPlanRoots.get(0).getValue() instanceof PathSemanticsReductionOperator) {
                    return false;
                }

                // ..otherwise, add this FP to our context set.
                contextSet.add(new FixedPointOperatorContext(opRef, sourceVariables, destVariables, downVariables,
                        pathVariable));
                break;
        }
        return false;
    }

    protected Predicate<LogicalVariable> createReductionPredicate(ILogicalOperator workingOp,
            List<LogicalVariable> groupingVars, IOptimizationContext context) throws AlgebricksException {
        // Iterate through each of our grouping variables.
        Map<LogicalVariable, List<VariableInReductionFinder>> variableToReductionMap = new HashMap<>();
        for (LogicalVariable groupingVar : groupingVars) {
            Map<LogicalVariable, EquivalenceClass> equivalenceMap = new LinkedHashMap<>();
            variableToReductionMap.put(groupingVar, new ArrayList<>());

            // Find our variable producer.
            new ProducerOperatorFinder(workingOp).searchAndUse(groupingVar, op -> {
                PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
                equivalenceMap.putAll(context.getEquivalenceClassMap(op));
                List<FunctionalDependency> fdList = context.getFDList(op);
                VariableInReductionFinder variableFinder = new VariableInReductionFinder(fdList, groupingVar);
                variableToReductionMap.get(groupingVar).add(variableFinder);
            }, () -> new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Variable not found?"));

            // Find variable equivalents to our producer.
            for (Map.Entry<LogicalVariable, EquivalenceClass> equivalenceClassEntry : equivalenceMap.entrySet()) {
                if (equivalenceClassEntry.getKey().equals(groupingVar)) {
                    for (LogicalVariable v : equivalenceClassEntry.getValue().getMembers()) {
                        if (!v.equals(groupingVar)) {
                            new ProducerOperatorFinder(workingOp).searchAndUse(v, op -> {
                                PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
                                VariableInReductionFinder variableFinder =
                                        new VariableInReductionFinder(context.getFDList(op), v);
                                variableToReductionMap.get(groupingVar).add(variableFinder);
                            }, () -> new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Variable not found?"));
                        }
                    }
                }
            }
        }
        return v -> {
            for (Map.Entry<LogicalVariable, List<VariableInReductionFinder>> e : variableToReductionMap.entrySet()) {
                for (VariableInReductionFinder variableInReductionFinder : e.getValue()) {
                    if (variableInReductionFinder.search(v)) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

    protected PathSemanticsReductionOperator createDistinctOp(FixedPointOperatorContext fpContext,
            IOptimizationContext optContext, ISemanticsOpSupplier distinctOpSupplier) throws AlgebricksException {
        FixedPointOperator fpOp = (FixedPointOperator) fpContext.fpOpRef.getValue();

        // Grab the primary key(s) of our source variable...
        List<LogicalVariable> sourcePrimaryKeys = new ArrayList<>();
        for (LogicalVariable sourceVariable : fpContext.startingVariables) {
            new PrimaryKeyVariableFinder(optContext, fpOp).searchAndUse(sourceVariable, sourcePrimaryKeys::addAll);
        }

        // ...and our destination variable (we'll need to update our PKs).
        List<LogicalVariable> recursiveDestVariables = fpContext.endingVariables.stream()
                .map(v -> fpOp.getInvertedRecursiveOutputMap().get(v)).collect(Collectors.toList());
        List<LogicalVariable> destPrimaryKeys = new ArrayList<>();
        for (LogicalVariable recursiveDestVariable : recursiveDestVariables) {
            ILogicalOperator rootOp = fpOp.getNestedPlans().get(0).getRoots().get(0).getValue();
            new PrimaryKeyVariableFinder(optContext, rootOp).searchAndUse(recursiveDestVariable,
                    destPrimaryKeys::addAll);
        }

        // Build up our DISTINCT expressions.
        List<Mutable<ILogicalExpression>> sourceExprRefList = new ArrayList<>();
        List<Mutable<ILogicalExpression>> destExprRefList = new ArrayList<>();
        Function<LogicalVariable, Mutable<ILogicalExpression>> f =
                v -> new MutableObject<>(new VariableReferenceExpression(v));
        if (sourcePrimaryKeys.isEmpty()) {
            sourceExprRefList.addAll(fpContext.startingVariables.stream().map(f).collect(Collectors.toList()));

        } else {
            for (LogicalVariable sourcePrimaryKey : sourcePrimaryKeys) {
                sourceExprRefList.add(f.apply(sourcePrimaryKey));
            }
        }
        if (destPrimaryKeys.isEmpty()) {
            destExprRefList.addAll(recursiveDestVariables.stream().map(f).collect(Collectors.toList()));

        } else {
            for (LogicalVariable destPrimaryKey : destPrimaryKeys) {
                destExprRefList.add(f.apply(destPrimaryKey));
            }
        }

        // Return our DISTINCT op.
        PathSemanticsReductionOperator distinctOp = distinctOpSupplier.build(sourceExprRefList, destExprRefList);
        distinctOp.setSourceLocation(fpOp.getSourceLocation());
        distinctOp.setExecutionMode(fpOp.getExecutionMode());
        return distinctOp;
    }

    @FunctionalInterface
    protected interface ISemanticsOpSupplier {
        PathSemanticsReductionOperator build(List<Mutable<ILogicalExpression>> sourceExprRefList,
                List<Mutable<ILogicalExpression>> destExprRefList);
    }
}
