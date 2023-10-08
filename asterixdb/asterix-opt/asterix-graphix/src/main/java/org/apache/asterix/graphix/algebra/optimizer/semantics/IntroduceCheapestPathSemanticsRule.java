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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.finder.InExpressionFunctionFinder;
import org.apache.asterix.graphix.algebra.finder.ProducerOperatorFinder;
import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Given a plan with a {@link FixedPointOperator}, we might be able to avoid enumerating all outputs if we can safely
 * reason that the {@code k} cheapest paths are required. If the following conditions are met, we will introduce a
 * {@link PathSemanticsReductionOperator} inside of our {@link FixedPointOperator}:
 * <ol>
 *   <li>A {@link FixedPointOperator} exists with {@code source}, {@code destination}, and {@code path}
 *   operator annotations...</li>
 *   <li>...and there exists a {@link GroupByOperator}</li> downstream that operates on our {@code source} and/or our
 *   {@code destination} variables (or variables / expressions that are functionally dependent on these variables),
 *   <b>BUT</b> not our {@code path} variable...</li>
 *   <li>...whose nested plan sorts by some function wrapped in {@link BuiltinFunctions#LEN} or
 *   {@link BuiltinFunctions#NUMERIC_ABS} (i.e. a non-negative value) in ascending order...</li>
 *   <li>...and where the nested plan contains a {@link LimitOperator} with a constant value.</li>
 * </ol>
 *
 * @apiNote {@link org.apache.hyracks.algebricks.rewriter.rules.SetExecutionModeRule} should run after this.
 */
public class IntroduceCheapestPathSemanticsRule extends AbstractIntroducePathSemanticsRule {
    private final static FunctionIdentifier[] NON_NEGATIVE_FUNCTION_WHITELIST = new FunctionIdentifier[] {
            GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER, BuiltinFunctions.LEN, BuiltinFunctions.NUMERIC_ABS };

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // If we haven't encountered any FIXED-POINT operators, we can exit early.
        if (contextSet.isEmpty()) {
            return false;
        }

        // Similarly, if we have already applied this transformation, exit early.
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // We are searching for GROUP-BY operators.
        boolean isChanged = false;
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator groupByOp = (GroupByOperator) op;

            // TODO (GLENN): Support GROUP-BYs with more than one group?
            if (groupByOp.getNestedPlans().size() > 1) {
                return false;
            }

            // Generate a function that tells us if a variable is dependent on this GROUP-BY.
            List<LogicalVariable> groupByVarList = groupByOp.getGroupByVarList();
            Predicate<LogicalVariable> doesDetermineGroupBy = createReductionPredicate(op, groupByVarList, context);

            // If there exists any GROUP-BY variable that is dependent on our path, then prune these FPs here.
            Set<FixedPointOperatorContext> survivingFPOps = contextSet.stream()
                    .filter(fc -> !doesDetermineGroupBy.test(fc.pathVariable)).collect(Collectors.toSet());

            // Iterate through our surviving FP operators.
            for (FixedPointOperatorContext fc : survivingFPOps) {
                boolean sourceDeterminesGrouping = fc.startingVariables.stream().allMatch(doesDetermineGroupBy);
                boolean destDeterminesGrouping = fc.endingVariables.stream().allMatch(doesDetermineGroupBy);
                boolean downDeterminesGrouping = fc.downVariables.stream().anyMatch(doesDetermineGroupBy);
                if (!sourceDeterminesGrouping || !(destDeterminesGrouping || downDeterminesGrouping)) {
                    continue;
                }
                FixedPointOperator fpOp = (FixedPointOperator) fc.fpOpRef.getValue();

                // We need two values: our K-value...
                Long kValue = getGroupByKValue(groupByOp);
                if (kValue == null) {
                    continue;
                }

                // ...and our weight-expression.
                WeightExprContext weightExprContext = getGroupByWeightExpr(groupByOp, context);
                if (weightExprContext == null) {
                    continue;
                }

                // Ensure that this plan refers to our working root.
                VariableUtilities.substituteVariablesInDescendantsAndSelf(weightExprContext.weightExprAssignOp,
                        fpOp.getInvertedRecursiveOutputMap(), context);

                // We will replace our NTS with the working root of our nested plan.
                Mutable<ILogicalOperator> rootOpRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
                ILogicalOperator previousRootOp = rootOpRef.getValue();
                weightExprContext.ntsOpRef.setValue(previousRootOp);

                // Set our semantics reduction operator as our new root.
                PathSemanticsReductionOperator underFpDistinctOp = createDistinctOp(fc, context, (e1, e2) -> {
                    ILogicalExpression v = weightExprContext.weightExprAssignOp.getExpressions().get(0).getValue();
                    return new PathSemanticsReductionOperator(e1, e2, new MutableObject<>(v.cloneExpression()), kValue);
                });
                underFpDistinctOp.getInputs().add(new MutableObject<>(weightExprContext.weightExprAssignOp));
                rootOpRef.setValue(underFpDistinctOp);
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(fpOp, context);
                context.addToDontApplySet(this, op);
                contextSet.remove(fc);
                isChanged = true;
            }
        }
        return isChanged;
    }

    private Long getGroupByKValue(GroupByOperator groupByOp) {
        ILogicalPlan groupByPlan = groupByOp.getNestedPlans().get(0);
        ILogicalOperator rootOp = groupByPlan.getRoots().get(0).getValue();
        Deque<ILogicalOperator> operatorStack = new ArrayDeque<>(List.of(rootOp));

        // Find a LIMIT op.
        LimitOperator limitOp = null;
        while (!operatorStack.isEmpty()) {
            ILogicalOperator workingOp = operatorStack.removeFirst();
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                operatorStack.addFirst(inputRef.getValue());
            }
            if (workingOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                LimitOperator workingLimitOp = (LimitOperator) workingOp;
                ILogicalExpression limitExpr = workingLimitOp.getMaxObjects().getValue();
                if (limitExpr != null && limitExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    limitOp = workingLimitOp;
                    break;
                }
            }
        }
        if (limitOp == null) {
            return null;
        }

        // Return the K-value associated with this LIMIT.
        ConstantExpression limitConstExpr = (ConstantExpression) limitOp.getMaxObjects().getValue();
        IAObject limitConstExprObject = ((AsterixConstantValue) limitConstExpr.getValue()).getObject();
        switch (limitConstExprObject.getType().getTypeTag()) {
            case TINYINT:
                return (long) ((AInt8) limitConstExprObject).getByteValue();
            case SMALLINT:
                return (long) ((AInt16) limitConstExprObject).getShortValue();
            case INTEGER:
                return (long) ((AInt32) limitConstExprObject).getIntegerValue();
            case BIGINT:
                return ((AInt64) limitConstExprObject).getLongValue();
            default:
                // We will only work with numeric integer values.
                return null;
        }
    }

    private WeightExprContext getGroupByWeightExpr(GroupByOperator groupByOp, IOptimizationContext ctx)
            throws AlgebricksException {
        ILogicalPlan groupByPlan = groupByOp.getNestedPlans().get(0);
        ILogicalOperator rootOp = groupByPlan.getRoots().get(0).getValue();
        Deque<ILogicalOperator> operatorStack = new ArrayDeque<>(List.of(rootOp));

        // First, find all ORDER ops (and their leading variables).
        List<Pair<ILogicalOperator, LogicalVariable>> orderByPairs = new ArrayList<>();
        while (!operatorStack.isEmpty()) {
            ILogicalOperator workingOp = operatorStack.removeFirst();
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                operatorStack.addFirst(inputRef.getValue());
            }
            if (workingOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                OrderOperator workingOrderOp = (OrderOperator) workingOp;
                Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p = workingOrderOp.getOrderExpressions().get(0);
                if (p.getFirst().getKind() == OrderOperator.IOrder.OrderKind.ASC) {
                    ILogicalExpression orderByExpr = p.getSecond().getValue();
                    if (orderByExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        VariableReferenceExpression orderByVarExpr = (VariableReferenceExpression) orderByExpr;
                        orderByPairs.add(new Pair<>(workingOrderOp, orderByVarExpr.getVariableReference()));
                    }
                }
            }
        }
        if (orderByPairs.isEmpty()) {
            return null;
        }

        // Next, find the producers of each variable.
        List<ILogicalOperator> orderByVariableProducerOps = new ArrayList<>();
        for (Pair<ILogicalOperator, LogicalVariable> orderByPair : orderByPairs) {
            ProducerOperatorFinder producerOperatorFinder = new ProducerOperatorFinder(orderByPair.getFirst());
            producerOperatorFinder.searchAndUse(orderByPair.getSecond(), orderByVariableProducerOps::add);
        }
        if (orderByVariableProducerOps.isEmpty()) {
            return null;
        }

        // We are looking for producer operators whose outer function-call is LEN or ABS.
        Pair<Integer, ILogicalOperator> indexOpPair = null;
        Iterator<ILogicalOperator> producerIt = orderByVariableProducerOps.iterator();
        while (producerIt.hasNext()) {
            ILogicalOperator producerOp = producerIt.next();
            if (producerOp instanceof AbstractAssignOperator) {
                AbstractAssignOperator assignOp = (AbstractAssignOperator) producerOp;
                Optional<Mutable<ILogicalExpression>> matchingExpr = assignOp.getExpressions().stream()
                        .filter(r -> new InExpressionFunctionFinder(NON_NEGATIVE_FUNCTION_WHITELIST).search(r))
                        .findFirst();
                if (matchingExpr.isPresent()) {
                    indexOpPair = new Pair<>(assignOp.getExpressions().indexOf(matchingExpr.get()), assignOp);
                    break;
                }
            }
            producerIt.remove();
        }
        if (indexOpPair == null) {
            return null;
        }

        // Clone our producer op subgraph. We need to place this in our fixed-point operator.
        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> subGraphCopy =
                OperatorManipulationUtil.deepCopyWithNewVars(indexOpPair.getSecond(), ctx);
        AbstractAssignOperator newAssignOp = (AbstractAssignOperator) subGraphCopy.getFirst();
        BitSet retainBitSet = new BitSet(newAssignOp.getExpressions().size());
        retainBitSet.set(indexOpPair.getFirst());
        OperatorManipulationUtil.retainAssignVariablesAndExpressions(newAssignOp.getVariables(),
                newAssignOp.getExpressions(), retainBitSet);

        // Locate the associated NTS operator.
        Deque<Mutable<ILogicalOperator>> operatorRefQueue = new ArrayDeque<>();
        operatorRefQueue.addLast(new MutableObject<>(newAssignOp));
        while (!operatorRefQueue.isEmpty()) {
            Mutable<ILogicalOperator> workingOpRef = operatorRefQueue.removeFirst();
            if (workingOpRef.getValue().getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                NestedTupleSourceOperator ntsOp = (NestedTupleSourceOperator) workingOpRef.getValue();
                if (ntsOp.getDataSourceReference().getValue().equals(groupByOp)) {
                    return new WeightExprContext(newAssignOp, workingOpRef);
                }
            }
            for (Mutable<ILogicalOperator> inputRef : workingOpRef.getValue().getInputs()) {
                operatorRefQueue.addLast(inputRef);
            }
        }
        throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find associated NTS?");
    }

    private static final class WeightExprContext {
        private final AbstractAssignOperator weightExprAssignOp;
        private final Mutable<ILogicalOperator> ntsOpRef;

        private WeightExprContext(AbstractAssignOperator weightExprAssignOp, Mutable<ILogicalOperator> ntsOpRef) {
            this.weightExprAssignOp = Objects.requireNonNull(weightExprAssignOp);
            this.ntsOpRef = Objects.requireNonNull(ntsOpRef);
        }
    }
}
