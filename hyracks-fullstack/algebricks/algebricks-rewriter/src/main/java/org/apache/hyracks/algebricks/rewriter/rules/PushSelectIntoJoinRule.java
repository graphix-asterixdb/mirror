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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushSelectIntoJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        Collection<LogicalVariable> joinLiveVarsLeft = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> joinLiveVarsRight = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> liveInOpsToPushLeft = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> liveInOpsToPushRight = new HashSet<LogicalVariable>();

        List<ILogicalOperator> pushedOnLeft = new ArrayList<ILogicalOperator>();
        List<ILogicalOperator> pushedOnRight = new ArrayList<ILogicalOperator>();
        List<ILogicalOperator> pushedOnEither = new ArrayList<ILogicalOperator>();
        LinkedList<ILogicalOperator> notPushedStack = new LinkedList<ILogicalOperator>();
        Collection<LogicalVariable> usedVars = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> producedVars = new HashSet<LogicalVariable>();

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator son = (AbstractLogicalOperator) opRef2.getValue();
        AbstractLogicalOperator op2 = son;
        boolean needToPushOps = false;
        while (son.isMap()) {
            needToPushOps = true;
            Mutable<ILogicalOperator> opRefLink = son.getInputs().get(0);
            son = (AbstractLogicalOperator) opRefLink.getValue();
        }

        if (son.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && son.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        boolean isLoj = son.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN;
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) son;

        Mutable<ILogicalOperator> joinBranchLeftRef = join.getInputs().get(0);
        Mutable<ILogicalOperator> joinBranchRightRef = join.getInputs().get(1);

        if (needToPushOps) {
            ILogicalOperator joinBranchLeft = joinBranchLeftRef.getValue();
            ILogicalOperator joinBranchRight = joinBranchRightRef.getValue();
            VariableUtilities.getLiveVariables(joinBranchLeft, joinLiveVarsLeft);
            VariableUtilities.getLiveVariables(joinBranchRight, joinLiveVarsRight);
            Mutable<ILogicalOperator> opIterRef;
            ILogicalOperator opIter = op2;
            while (opIter != join) {
                LogicalOperatorTag tag = opIter.getOperatorTag();
                if (tag == LogicalOperatorTag.PROJECT) {
                    notPushedStack.addFirst(opIter);
                } else {
                    VariableUtilities.getUsedVariables(opIter, usedVars);
                    VariableUtilities.getProducedVariables(opIter, producedVars);
                    if (usedVars.size() == 0) {
                        pushedOnEither.add(opIter);
                    } else if (joinLiveVarsLeft.containsAll(usedVars)) {
                        pushedOnLeft.add(opIter);
                        liveInOpsToPushLeft.addAll(producedVars);
                    } else if (joinLiveVarsRight.containsAll(usedVars)) {
                        pushedOnRight.add(opIter);
                        liveInOpsToPushRight.addAll(producedVars);
                    } else {
                        return false;
                    }
                }
                opIterRef = opIter.getInputs().get(0);
                opIter = opIterRef.getValue();
            }
            if (isLoj && pushedOnLeft.isEmpty()) {
                return false;
            }
        }

        boolean intersectsAllBranches = true;
        boolean[] intersectsBranch = new boolean[join.getInputs().size()];
        LinkedList<LogicalVariable> selectVars = new LinkedList<>();
        select.getCondition().getValue().getUsedVariables(selectVars);
        int i = 0;
        for (Mutable<ILogicalOperator> branch : join.getInputs()) {
            LinkedList<LogicalVariable> branchVars = new LinkedList<>();
            VariableUtilities.getLiveVariables(branch.getValue(), branchVars);
            if (i == 0) {
                branchVars.addAll(liveInOpsToPushLeft);
            } else {
                branchVars.addAll(liveInOpsToPushRight);
            }
            if (OperatorPropertiesUtil.disjoint(selectVars, branchVars)) {
                intersectsAllBranches = false;
            } else {
                intersectsBranch[i] = true;
            }
            i++;
        }
        if (!intersectsBranch[0] && !intersectsBranch[1]) {
            return false;
        }
        boolean planChanged;
        if (needToPushOps) {
            //We should push independent ops into the first branch that the selection depends on
            planChanged =
                    pushOps(pushedOnEither, intersectsBranch[0] ? joinBranchLeftRef : joinBranchRightRef, context);
            planChanged |= pushOps(pushedOnLeft, joinBranchLeftRef, context);
            planChanged |= pushOps(pushedOnRight, joinBranchRightRef, context);
        } else {
            planChanged = false;
        }
        if (intersectsAllBranches) {
            // add condition to the join condition only if we have IJ
            if (isLoj) {
                notPushedStack.addFirst(select);
            } else {
                addCondToJoin(select, join, context);
                planChanged = true;
            }
        } else { // push down
            Iterator<Mutable<ILogicalOperator>> branchIter = join.getInputs().iterator();
            ILogicalExpression selectCondition = select.getCondition().getValue();
            boolean lojToInner = false;
            for (int j = 0; j < intersectsBranch.length; j++) {
                Mutable<ILogicalOperator> branch = branchIter.next();
                boolean inter = intersectsBranch[j];
                if (!inter) {
                    continue;
                }
                if (j > 0 && isLoj) {
                    // if a LOJ and the select condition is not-missing filtering,
                    // we rewrite LOJ to IJ for this case.
                    FunctionIdentifier isMissingNullFunction = OperatorPropertiesUtil
                            .getIsMissingNullFunction(((LeftOuterJoinOperator) join).getMissingValue());
                    if (containsNotMissingFiltering(selectCondition, isMissingNullFunction)) {
                        lojToInner = true;
                    }
                    // Do not push conditions into the right branch of a LOJ;
                    notPushedStack.addFirst(select);
                } else {
                    // Conditions for the left branch for IJ/LOJ or
                    // for the right branch of IJ can always be pushed into that branch.
                    // We don't push conditions into the right branch of LOJ at this point.
                    copySelectToBranch(select, branch, context);
                    planChanged = true;
                }
            }
            if (lojToInner) {
                // Rewrites left outer join  to inner join.
                InnerJoinOperator innerJoin = new InnerJoinOperator(join.getCondition());
                innerJoin.getInputs().addAll(join.getInputs());
                join = innerJoin;
                context.computeAndSetTypeEnvironmentForOperator(join);
                planChanged = true;
            }
        }

        planChanged |= applyNonPushed(opRef, notPushedStack, join, context);
        return planChanged;
    }

    private boolean applyNonPushed(Mutable<ILogicalOperator> opRef, LinkedList<ILogicalOperator> notPushedStack,
            ILogicalOperator top, IOptimizationContext context) throws AlgebricksException {
        switch (notPushedStack.size()) {
            case 0:
                if (opRef.getValue() == top) {
                    return false;
                }
                opRef.setValue(top);
                return true;
            case 1:
                ILogicalOperator notPushedOp = notPushedStack.peek();
                if (opRef.getValue() == notPushedOp && opRef.getValue().getInputs().get(0).getValue() == top) {
                    return false;
                }
                // fall thru to 'default'
            default:
                for (ILogicalOperator npOp : notPushedStack) {
                    List<Mutable<ILogicalOperator>> npInpList = npOp.getInputs();
                    npInpList.clear();
                    npInpList.add(new MutableObject<>(top));
                    context.computeAndSetTypeEnvironmentForOperator(npOp);
                    top = npOp;
                }
                opRef.setValue(top);
                return true;
        }
    }

    private boolean pushOps(List<ILogicalOperator> opList, Mutable<ILogicalOperator> joinBranch,
            IOptimizationContext context) throws AlgebricksException {
        if (opList.isEmpty()) {
            return false;
        }
        ILogicalOperator topOp = joinBranch.getValue();
        ListIterator<ILogicalOperator> iter = opList.listIterator(opList.size());
        while (iter.hasPrevious()) {
            ILogicalOperator op = iter.previous();
            List<Mutable<ILogicalOperator>> opInpList = op.getInputs();
            opInpList.clear();
            opInpList.add(new MutableObject<>(topOp));
            topOp = op;
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        joinBranch.setValue(topOp);
        return true;
    }

    private static void addCondToJoin(SelectOperator select, AbstractBinaryJoinOperator join,
            IOptimizationContext context) {
        ILogicalExpression cond = join.getCondition().getValue();
        if (OperatorPropertiesUtil.isAlwaysTrueCond(cond)) { // the join was a product
            join.getCondition().setValue(select.getCondition().getValue());
        } else {
            boolean bAddedToConj = false;
            if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression fcond = (AbstractFunctionCallExpression) cond;
                if (fcond.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                    AbstractFunctionCallExpression newCond = new ScalarFunctionCallExpression(
                            context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND));
                    newCond.getArguments().add(select.getCondition());
                    newCond.getArguments().addAll(fcond.getArguments());
                    join.getCondition().setValue(newCond);
                    bAddedToConj = true;
                }
            }
            if (!bAddedToConj) {
                AbstractFunctionCallExpression newCond = new ScalarFunctionCallExpression(
                        context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND),
                        select.getCondition(), new MutableObject<>(join.getCondition().getValue()));
                join.getCondition().setValue(newCond);
            }
        }
    }

    private static void copySelectToBranch(SelectOperator select, Mutable<ILogicalOperator> branch,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator newSelect = new SelectOperator(select.getCondition(), select.getRetainMissingAsValue(),
                select.getMissingPlaceholderVariable());
        Mutable<ILogicalOperator> newRef = new MutableObject<>(branch.getValue());
        newSelect.getInputs().add(newRef);
        branch.setValue(newSelect);
        context.computeAndSetTypeEnvironmentForOperator(newSelect);
    }

    /**
     * Whether the expression contains a not-missing filtering
     *
     * @param expr
     * @param isMissingNullFunId
     * @return true if the expression contains a not-missing filtering function call; false otherwise.
     */
    private boolean containsNotMissingFiltering(ILogicalExpression expr, FunctionIdentifier isMissingNullFunId) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        ScalarFunctionCallExpression func = (ScalarFunctionCallExpression) expr;
        if (func.getFunctionIdentifier() == AlgebricksBuiltinFunctions.AND) {
            for (Mutable<ILogicalExpression> argumentRef : func.getArguments()) {
                if (containsNotMissingFiltering(argumentRef.getValue(), isMissingNullFunId)) {
                    return true;
                }
            }
            return false;
        }
        if (func.getFunctionIdentifier() != AlgebricksBuiltinFunctions.NOT) {
            return false;
        }
        ILogicalExpression arg = func.getArguments().get(0).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        ScalarFunctionCallExpression func2 = (ScalarFunctionCallExpression) arg;
        return func2.getFunctionIdentifier().equals(isMissingNullFunId);
    }
}
