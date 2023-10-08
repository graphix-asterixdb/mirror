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
package org.apache.asterix.graphix.algebra.optimizer.normalize;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If we can reason that an {@link AssignOperator} binding a {@link BuiltinFunctions#CLOSED_RECORD_CONSTRUCTOR}
 * (or {@link BuiltinFunctions#OPEN_RECORD_CONSTRUCTOR}) is not required (i.e. we can use their bottommost equivalent
 * instead of a field-access to the record itself), then replace all usages of the {@link AssignOperator}. We assume
 * that all {@link AssignOperator} have not yet been consolidated.
 */
public class ExtractFromRecordConstructorRule implements IAlgebraicRewriteRule {
    private final PreVisitTransform preVisitTransform = new PreVisitTransform();
    private final PostVisitTransform postVisitTransform = new PostVisitTransform();
    private final Deque<ILogicalOperator> upstreamOperatorStack = new ArrayDeque<>();
    private final Map<LogicalVariable, LogicalVariable> variableMap = new LinkedHashMap<>();

    // We might need to introduce operators below ASSIGNs.
    private IOptimizationContext optContext;
    private Mutable<ILogicalOperator> workingOpRef;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        workingOpRef = opRef;
        optContext = context;

        // Try to change our expression.
        boolean hasChanged = false;
        if (!optContext.checkIfInDontApplySet(this, workingOpRef.getValue())
                && workingOpRef.getValue().acceptExpressionTransform(preVisitTransform)) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(workingOpRef.getValue(), optContext);
            hasChanged = true;
        }

        // We want to keep track of operators we have already seen.
        upstreamOperatorStack.addLast(workingOpRef.getValue());
        return hasChanged;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        workingOpRef = opRef;
        optContext = context;

        // If we have an ASSIGN, handle any variable remappings...
        if (workingOpRef.getValue().getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) workingOpRef.getValue();
            OptionalInt isModified = IntStream.range(0, assignOp.getExpressions().size()).filter(i -> {
                Mutable<ILogicalExpression> exprRef = assignOp.getExpressions().get(i);
                if (exprRef.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    return false;
                }
                VariableReferenceExpression varExpr = (VariableReferenceExpression) exprRef.getValue();
                return variableMap.containsKey(varExpr.getVariableReference());
            }).peek(i -> {
                ILogicalExpression matchingExpr = assignOp.getExpressions().get(i).getValue();
                VariableReferenceExpression sourceVarExpr = (VariableReferenceExpression) matchingExpr;
                LogicalVariable remappedVariable = assignOp.getVariables().get(i);
                LogicalVariable sourceVariable = sourceVarExpr.getVariableReference();

                // Generate a new set of replacement map values.
                Map<ILogicalExpression, ILogicalExpression> additionalMap = new HashMap<>();
                Map<ILogicalExpression, ILogicalExpression> replacementMap = postVisitTransform.replacementMap;
                for (Map.Entry<ILogicalExpression, ILogicalExpression> e : replacementMap.entrySet()) {
                    ILogicalExpression replacementExpr = e.getValue();
                    ILogicalExpression sourceExpr = e.getKey().cloneExpression();
                    sourceExpr.substituteVar(sourceVariable, remappedVariable);
                    additionalMap.put(sourceExpr, replacementExpr);
                }
                replacementMap.putAll(additionalMap);
            }).findAny();
            if (isModified.isPresent()) {
                return false;
            }
        }

        // ...otherwise, try to change our expression.
        boolean hasChanged = false;
        if (workingOpRef.getValue().acceptExpressionTransform(postVisitTransform)) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(workingOpRef.getValue(), optContext);
            hasChanged = true;
        }

        // If this is the root operator, then remove our state.
        switch (opRef.getValue().getOperatorTag()) {
            case DISTRIBUTE_RESULT:
            case DELEGATE_OPERATOR:
            case SINK:
                upstreamOperatorStack.clear();
                postVisitTransform.replacementMap.clear();
        }
        return hasChanged;
    }

    private final class PreVisitTransform implements ILogicalExpressionReferenceTransform {
        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression workingExpr = exprRef.getValue();

            // Do we have a record constructor?
            if (workingExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) workingExpr;
            if (!funcCallExpr.getFunctionIdentifier().equals(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)
                    && !funcCallExpr.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
                return false;
            }
            List<Mutable<ILogicalExpression>> funcCallArguments = funcCallExpr.getArguments();

            // We have a record constructor. Grab the variable bound to this record.
            AssignOperator workingAssignOp = (AssignOperator) workingOpRef.getValue();
            LogicalVariable workingVar = workingAssignOp.getVariables().get(0);

            // Walk through our upstream operators, find all users of this variable.
            List<ILogicalOperator> workingVarUsers = new ArrayList<>();
            for (ILogicalOperator upstreamOp : upstreamOperatorStack) {
                List<LogicalVariable> usedVariables = new ArrayList<>();
                VariableUtilities.getUsedVariables(upstreamOp, usedVariables);
                if (usedVariables.contains(workingVar)) {
                    workingVarUsers.add(upstreamOp);
                }
            }

            // Filter out all users that access this variable through a field-access.
            for (ILogicalOperator userOp : workingVarUsers) {
                MutableBoolean isNotFieldAccessFlag = new MutableBoolean(false);
                ILogicalExpressionReferenceTransform isNotFieldAccess = innerExprRef -> {
                    ILogicalExpression innerExpr = innerExprRef.getValue();
                    AbstractFunctionCallExpression callExpr;
                    switch (innerExpr.getExpressionTag()) {
                        case FUNCTION_CALL:
                            callExpr = (AbstractFunctionCallExpression) innerExpr;
                            FunctionIdentifier functionId = callExpr.getFunctionIdentifier();
                            if (!functionId.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)
                                    && !functionId.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                                break;
                            }
                            ILogicalExpression firstArgExpr = callExpr.getArguments().get(0).getValue();
                            if (firstArgExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                                break;
                            }
                            VariableReferenceExpression argVarExpr = (VariableReferenceExpression) firstArgExpr;
                            if (argVarExpr.getVariableReference().equals(workingVar)) {
                                return false;
                            }
                            break;

                        case VARIABLE:
                            VariableReferenceExpression varExpr = (VariableReferenceExpression) innerExpr;
                            if (varExpr.getVariableReference().equals(workingVar)) {
                                isNotFieldAccessFlag.setTrue();
                            }
                            return false;

                        default: // case CONSTANT:
                            return false;
                    }

                    // We are working with a function call. Recurse on our arguments.
                    for (Mutable<ILogicalExpression> argExprRef : callExpr.getArguments()) {
                        this.transform(argExprRef);
                    }

                    // We won't be transforming anything, so we'll always return false.
                    return false;
                };
                userOp.acceptExpressionTransform(isNotFieldAccess);
                if (isNotFieldAccessFlag.booleanValue()) {
                    // We cannot remove this object constructor.
                    return false;
                }
            }

            // We can remove this record constructor. Walk through the value expressions of our record constructor.
            Iterator<Mutable<ILogicalExpression>> it = funcCallArguments.iterator();
            for (int i = 0; it.hasNext(); i++) {
                Mutable<ILogicalExpression> fieldNameExprRef = it.next();
                Mutable<ILogicalExpression> fieldValueExprRef = it.next();

                // We will extract this to a new ASSIGN.
                LogicalVariable replacementVar = optContext.newVar();
                Mutable<ILogicalExpression> fieldValueExprRefCopy = new MutableObject<>(fieldValueExprRef.getValue());
                AssignOperator assignOp = new AssignOperator(replacementVar, fieldValueExprRefCopy);
                assignOp.getInputs().add(new MutableObject<>(workingOpRef.getValue().getInputs().get(0).getValue()));
                workingOpRef.getValue().getInputs().clear();
                workingOpRef.getValue().getInputs().add(new MutableObject<>(assignOp));

                // Replace all usages that access this record, with this variable.
                VariableReferenceExpression replacementVarExpr = new VariableReferenceExpression(replacementVar);
                fieldValueExprRef.setValue(replacementVarExpr);
                ScalarFunctionCallExpression fieldAccessByName = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                        List.of(new MutableObject<>(new VariableReferenceExpression(workingVar)),
                                new MutableObject<>(fieldNameExprRef.getValue())));
                ScalarFunctionCallExpression fieldAccessByIndex = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        List.of(new MutableObject<>(new VariableReferenceExpression(workingVar)),
                                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(i))))));
                postVisitTransform.replacementMap.put(fieldAccessByName, replacementVarExpr);
                postVisitTransform.replacementMap.put(fieldAccessByIndex, replacementVarExpr);
                variableMap.put(workingVar, replacementVar);
            }
            optContext.addToDontApplySet(ExtractFromRecordConstructorRule.this, workingOpRef.getValue());
            return true;
        }
    }

    private static final class PostVisitTransform implements ILogicalExpressionReferenceTransform {
        private final Map<ILogicalExpression, ILogicalExpression> replacementMap = new LinkedHashMap<>();

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) {
            ILogicalExpression workingExpr = exprRef.getValue();

            // Check if we can directly replace this expression...
            if (replacementMap.containsKey(workingExpr)) {
                exprRef.setValue(replacementMap.get(workingExpr).cloneExpression());
                return true;
            }

            // ...if we have a function, then recurse into the arguments...
            if (workingExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) workingExpr;
                boolean isChanged = false;
                for (Mutable<ILogicalExpression> argRef : funcCallExpr.getArguments()) {
                    isChanged |= transform(argRef);
                }
                return isChanged;
            }

            // ...otherwise, we cannot replace this.
            return false;
        }
    }
}
