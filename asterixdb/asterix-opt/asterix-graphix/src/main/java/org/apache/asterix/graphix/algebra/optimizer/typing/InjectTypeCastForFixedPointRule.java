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
package org.apache.asterix.graphix.algebra.optimizer.typing;

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.CREATE_NEW_ZERO_HOP_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.IS_DISTINCT_;
import static org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities.substituteVariablesInDescendantsAndSelf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.graphix.algebra.finder.DownstreamFunctionFinder;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Similar to {@link org.apache.asterix.optimizer.rules.InjectTypeCastForUnionRule}, this rule will add type casts if
 * an ANCHOR variable's type differs from a RECURSIVE variable's type.
 */
public class InjectTypeCastForFixedPointRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
        Mutable<ILogicalOperator> anchorOpRef = fpOp.getInputs().get(0);
        Mutable<ILogicalOperator> recursiveOpRef = fpOp.getNestedPlans().get(0).getRoots().get(0);

        // Find our tail op.
        RecursiveTailOperator tailOp = null;
        Deque<ILogicalOperator> operatorQueue = new ArrayDeque<>();
        operatorQueue.addLast(recursiveOpRef.getValue());
        while (!operatorQueue.isEmpty()) {
            ILogicalOperator workingOp = operatorQueue.removeFirst();
            if (workingOp.getOperatorTag() == LogicalOperatorTag.RECURSIVE_TAIL) {
                tailOp = (RecursiveTailOperator) workingOp;
                break;
            }
            for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                operatorQueue.addLast(inputRef.getValue());
            }
        }
        if (tailOp == null) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "RECURSIVE_TAIL not found?");
        }

        // Grab our type environments.
        IVariableTypeEnvironment fpTypeEnv = context.getOutputTypeEnvironment(fpOp);
        IVariableTypeEnvironment anchorTypeEnv = context.getOutputTypeEnvironment(anchorOpRef.getValue());
        IVariableTypeEnvironment recursiveTypeEnv = context.getOutputTypeEnvironment(recursiveOpRef.getValue());

        // Iterate through our output mapping and find variables we need to cast.
        Map<LogicalVariable, FixedPointTypeContext> anchorContextMap = new HashMap<>();
        Map<LogicalVariable, FixedPointTypeContext> recursiveContextMap = new HashMap<>();
        for (LogicalVariable outputVariable : fpOp.getOutputVariableList()) {
            LogicalVariable anchorVariable = fpOp.getInvertedAnchorOutputMap().get(outputVariable);
            LogicalVariable recursiveVariable = fpOp.getInvertedRecursiveOutputMap().get(outputVariable);

            // Does this variable require a cast?
            IAType outputType = (IAType) fpTypeEnv.getVarType(outputVariable);
            IAType anchorType = (IAType) anchorTypeEnv.getVarType(anchorVariable);
            IAType recursiveType = (IAType) recursiveTypeEnv.getVarType(recursiveVariable);
            if (TypeResolverUtil.needsCast(outputType, anchorType)) {
                anchorContextMap.put(anchorVariable,
                        new FixedPointTypeContext(context.newVar(), anchorVariable, anchorType, outputType));
            }
            if (TypeResolverUtil.needsCast(outputType, recursiveType)) {
                recursiveContextMap.put(recursiveVariable,
                        new FixedPointTypeContext(context.newVar(), recursiveVariable, recursiveType, outputType));
            }
        }
        if (anchorContextMap.isEmpty() && recursiveContextMap.isEmpty()) {
            return false;
        }

        // Update our variable mappings for our FP and recursive tail. Our recursive head remains untouched.
        Map<LogicalVariable, LogicalVariable> replacementAnchorMap = anchorContextMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().mappedVariable));
        Map<LogicalVariable, LogicalVariable> replacementRecursiveMap = recursiveContextMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().mappedVariable));
        substituteVariablesInFixedPoint(fpOp, replacementAnchorMap);
        substituteVariablesInFixedPoint(fpOp, replacementRecursiveMap);
        VariableUtilities.substituteVariables(tailOp, replacementAnchorMap, context);

        // Add our cast functions for both our anchor input and recursive input.
        for (FixedPointTypeContext fixedPointTypeContext : anchorContextMap.values()) {
            DownstreamFunctionFinder createPathFinder = new DownstreamFunctionFinder(CREATE_NEW_ZERO_HOP_PATH);
            placeAssignOp(fixedPointTypeContext, context, anchorOpRef, createPathFinder);
        }
        for (FixedPointTypeContext fixedPointTypeContext : recursiveContextMap.values()) {
            DownstreamFunctionFinder cycleConstraintFinder = new DownstreamFunctionFinder(IS_DISTINCT_);
            placeAssignOp(fixedPointTypeContext, context, recursiveOpRef, cycleConstraintFinder);
        }
        return true;
    }

    private void placeAssignOp(FixedPointTypeContext fpContext, IOptimizationContext optContext,
            Mutable<ILogicalOperator> inputOpRef, DownstreamFunctionFinder callFinder) throws AlgebricksException {
        LogicalVariable inputVariable = fpContext.inputVariable;
        LogicalVariable mappedVariable = fpContext.mappedVariable;

        // Build our new ASSIGN op.
        Mutable<ILogicalExpression> castExpr = new MutableObject<>(fpContext.createCastFunction());
        AssignOperator newAssignOp = new AssignOperator(fpContext.mappedVariable, castExpr);
        newAssignOp.setExecutionMode(inputOpRef.getValue().getExecutionMode());
        newAssignOp.setSourceLocation(inputOpRef.getValue().getSourceLocation());

        // Is our input the variable producer?
        List<LogicalVariable> inputProducedVariables = new ArrayList<>();
        VariableUtilities.getProducedVariables(inputOpRef.getValue(), inputProducedVariables);
        if (inputProducedVariables.contains(inputVariable)) {
            ILogicalOperator inputOp = inputOpRef.getValue();
            newAssignOp.getInputs().add(new MutableObject<>(inputOp));
            inputOpRef.setValue(newAssignOp);
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(newAssignOp, optContext);

        } else {
            // Otherwise, use our function call finder.
            if (!callFinder.search(inputOpRef.getValue())) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find function call!");
            }
            Mutable<ILogicalOperator> containingInputOpRef = callFinder.getContainingOp().getInputs().remove(0);
            substituteVariablesInDescendantsAndSelf(inputOpRef.getValue(), inputVariable, mappedVariable, optContext);
            newAssignOp.getInputs().add(new MutableObject<>(containingInputOpRef.getValue()));
            callFinder.getContainingOp().getInputs().add(0, new MutableObject<>(newAssignOp));
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(inputOpRef.getValue(), optContext);
        }
    }

    private void substituteVariablesInFixedPoint(FixedPointOperator fpOp,
            Map<LogicalVariable, LogicalVariable> replacementMap) {
        // We just want to replace our variable maps in our fixed point (not our nested plan).
        for (Map.Entry<LogicalVariable, LogicalVariable> replacementEntry : replacementMap.entrySet()) {
            Function<Map.Entry<LogicalVariable, LogicalVariable>, Pair<LogicalVariable, LogicalVariable>> f = e -> {
                LogicalVariable replacementKey = e.getKey();
                LogicalVariable replacementValue = e.getValue();
                if (replacementKey == replacementEntry.getKey()) {
                    replacementKey = replacementEntry.getValue();
                }
                if (replacementValue == replacementEntry.getKey()) {
                    replacementValue = replacementEntry.getValue();
                }
                return new Pair<>(replacementKey, replacementValue);
            };
            Map<LogicalVariable, LogicalVariable> replacementAnchorMap = fpOp.getAnchorOutputMap().entrySet().stream()
                    .map(f).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            Map<LogicalVariable, LogicalVariable> replacementRecursiveMap = fpOp.getRecursiveOutputMap().entrySet()
                    .stream().map(f).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            fpOp.replaceOutputMaps(replacementAnchorMap, replacementRecursiveMap);
        }
    }

    private static final class FixedPointTypeContext {
        private final LogicalVariable mappedVariable;
        private final LogicalVariable inputVariable;
        private final IAType inputType;
        private final IAType outputType;

        private FixedPointTypeContext(LogicalVariable mappedVariable, LogicalVariable inputVariable, IAType inputType,
                IAType outputType) {
            this.mappedVariable = Objects.requireNonNull(mappedVariable);
            this.inputVariable = Objects.requireNonNull(inputVariable);
            this.inputType = Objects.requireNonNull(inputType);
            this.outputType = Objects.requireNonNull(outputType);
        }

        private ILogicalExpression createCastFunction() throws AlgebricksException {
            BuiltinFunctionInfo fi = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.CAST_TYPE);
            VariableReferenceExpression inputVarRef = new VariableReferenceExpression(inputVariable);
            List<Mutable<ILogicalExpression>> funcCallExprArgs = new ArrayList<>();
            funcCallExprArgs.add(new MutableObject<>(inputVarRef));
            ScalarFunctionCallExpression funcCallExpr = new ScalarFunctionCallExpression(fi, funcCallExprArgs);
            TypeCastUtils.setRequiredAndInputTypes(funcCallExpr, outputType, inputType);
            return funcCallExpr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FixedPointTypeContext that = (FixedPointTypeContext) o;
            return Objects.equals(mappedVariable, that.mappedVariable)
                    && Objects.equals(inputVariable, that.inputVariable) && Objects.equals(inputType, that.inputType)
                    && Objects.equals(outputType, that.outputType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mappedVariable, inputVariable, inputType, outputType);
        }
    }
}
