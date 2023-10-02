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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class FixedPointOperator extends AbstractOperatorWithNestedPlans {
    private final Map<LogicalVariable, LogicalVariable> anchorOutputMap;
    private final Map<LogicalVariable, LogicalVariable> recursiveOutputMap;

    // The following are not passed in, but are generated.
    private Map<LogicalVariable, LogicalVariable> invertedAnchorOutputMap;
    private Map<LogicalVariable, LogicalVariable> invertedRecursiveOutputMap;

    // Use this to propagate any changes to variables when this operator is duplicated.
    private IIsomorphismCopyCallback onIsomorphicCopyCallback;

    public FixedPointOperator(Map<LogicalVariable, LogicalVariable> anchorOutputMap,
            Map<LogicalVariable, LogicalVariable> recursiveOutputMap, List<ILogicalPlan> plansInLoop) {
        super(Objects.requireNonNull(plansInLoop));
        this.anchorOutputMap = Objects.requireNonNull(anchorOutputMap);
        this.recursiveOutputMap = Objects.requireNonNull(recursiveOutputMap);
        if (anchorOutputMap.values().stream().anyMatch(v -> !recursiveOutputMap.containsValue(v))) {
            throw new IllegalArgumentException("Output maps should have the exact same output values!");
        }

        // Populate our inverted maps.
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> aStream = anchorOutputMap.entrySet().stream();
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> rStream = recursiveOutputMap.entrySet().stream();
        this.invertedAnchorOutputMap = aStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.invertedRecursiveOutputMap = rStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public Map<LogicalVariable, LogicalVariable> getAnchorOutputMap() {
        return Collections.unmodifiableMap(anchorOutputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getRecursiveOutputMap() {
        return Collections.unmodifiableMap(recursiveOutputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getInvertedAnchorOutputMap() {
        return Collections.unmodifiableMap(invertedAnchorOutputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getInvertedRecursiveOutputMap() {
        return Collections.unmodifiableMap(invertedRecursiveOutputMap);
    }

    public List<LogicalVariable> getOutputVariableList() {
        return new ArrayList<>(anchorOutputMap.values());
    }

    public void replaceOutputMaps(Map<LogicalVariable, LogicalVariable> newAnchorOutputMap,
            Map<LogicalVariable, LogicalVariable> newRecursiveOutputMap) {
        this.anchorOutputMap.clear();
        this.recursiveOutputMap.clear();
        this.anchorOutputMap.putAll(Objects.requireNonNull(newAnchorOutputMap));
        this.recursiveOutputMap.putAll(Objects.requireNonNull(newRecursiveOutputMap));
        if (newAnchorOutputMap.values().stream().anyMatch(v -> !newRecursiveOutputMap.containsValue(v))) {
            throw new IllegalArgumentException("Output maps should have the exact same output values!");
        }

        // Repopulate our inverted maps.
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> aStream = anchorOutputMap.entrySet().stream();
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> rStream = recursiveOutputMap.entrySet().stream();
        this.invertedAnchorOutputMap = aStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.invertedRecursiveOutputMap = rStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    // TODO (GLENN): Find a cleaner / better way to do this. :-)
    public void setOnIsomorphicCopyCallback(IIsomorphismCopyCallback onIsomorphicCopyCallback) {
        this.onIsomorphicCopyCallback = Objects.requireNonNull(onIsomorphicCopyCallback);
    }

    public IIsomorphismCopyCallback getOnIsomorphicCopyCallback() {
        return onIsomorphicCopyCallback;
    }

    public void applyOnIsomorphicCopyCallback(Map<LogicalVariable, LogicalVariable> inputToOutputMap,
            FixedPointOperator newFpOp) throws AlgebricksException {
        this.onIsomorphicCopyCallback.accept(inputToOutputMap, this, newFpOp);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitFixedPointOperator(this, arg);
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<>();

        // Add all of our output variables...
        schema.addAll(getOutputVariableList());

        // ...and everything from our anchor input.
        ILogicalOperator anchorInputOp = inputs.get(0).getValue();
        schema.addAll(anchorInputOp.getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        // Note: this policy should only be used when translating the ANCHOR input (not for our nested plans).
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
                Set<LogicalVariable> visitedSet = new HashSet<>();
                for (LogicalVariable sourceVariable : sources[0]) {
                    if (anchorOutputMap.containsKey(sourceVariable) && !visitedSet.contains(sourceVariable)) {
                        target.addVariable(anchorOutputMap.get(sourceVariable));
                        visitedSet.add(sourceVariable);

                    } else {
                        target.addVariable(sourceVariable);
                    }
                }
            }
        };
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ILogicalOperator recursiveInputOp = nestedPlans.get(0).getRoots().get(0).getValue();
        ILogicalOperator anchorInputOp = inputs.get(0).getValue();

        // What we produce in-the-loop and before-the-loop might be typed differently.
        IConflictingTypeResolver conflictingTypeResolver = ctx.getConflictingTypeResolver();
        IVariableTypeEnvironment typeEnv = createNestedPlansPropagatingTypeEnvironment(ctx, true);
        IVariableTypeEnvironment anchorEnv = ctx.getOutputTypeEnvironment(anchorInputOp);
        IVariableTypeEnvironment recursiveEnv = ctx.getOutputTypeEnvironment(recursiveInputOp);
        for (LogicalVariable outputVariable : getOutputVariableList()) {
            Object anchorVarType = anchorEnv.getVarType(invertedAnchorOutputMap.get(outputVariable));
            Object recursiveVarType = recursiveEnv.getVarType(invertedRecursiveOutputMap.get(outputVariable));
            if (Objects.equals(anchorVarType, recursiveVarType)) {
                typeEnv.setVarType(outputVariable, recursiveVarType);
            } else {
                typeEnv.setVarType(outputVariable, conflictingTypeResolver.resolve(anchorVarType, recursiveVarType));
            }
        }
        return typeEnv;
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        vars.addAll(anchorOutputMap.keySet());
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        vars.addAll(getOutputVariableList());
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.FIXED_POINT;
    }

    @FunctionalInterface
    public interface IIsomorphismCopyCallback {
        void accept(Map<LogicalVariable, LogicalVariable> inputToOutputMap, FixedPointOperator inputFpOp,
                FixedPointOperator outputFpOp) throws AlgebricksException;
    }
}
