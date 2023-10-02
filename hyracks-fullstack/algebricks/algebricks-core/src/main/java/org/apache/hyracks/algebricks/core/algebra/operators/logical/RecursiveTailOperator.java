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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class RecursiveTailOperator extends AbstractLogicalOperator {
    private final Map<LogicalVariable, LogicalVariable> anchorInputMap;
    private final Map<LogicalVariable, LogicalVariable> recursiveInputMap;
    private RecursiveHeadOperator headOp;
    private FixedPointOperator fpOp;

    // The following are not passed in, but are generated.
    private Map<LogicalVariable, LogicalVariable> invertedAnchorInputMap;
    private Map<LogicalVariable, LogicalVariable> invertedRecursiveInputMap;

    public RecursiveTailOperator(Map<LogicalVariable, LogicalVariable> anchorInputMap,
            Map<LogicalVariable, LogicalVariable> recursiveInputMap) {
        this.anchorInputMap = Objects.requireNonNull(anchorInputMap);
        this.recursiveInputMap = Objects.requireNonNull(recursiveInputMap);
        if (anchorInputMap.values().stream().anyMatch(v -> !recursiveInputMap.containsValue(v))) {
            throw new IllegalArgumentException("Input maps should have the exact same output values!");
        }

        // Populate our inverted maps.
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> aStream = anchorInputMap.entrySet().stream();
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> rStream = recursiveInputMap.entrySet().stream();
        this.invertedAnchorInputMap = aStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.invertedRecursiveInputMap = rStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public RecursiveTailOperator(RecursiveTailOperator inputTailOp) {
        this.anchorInputMap = new HashMap<>(inputTailOp.anchorInputMap);
        this.recursiveInputMap = new HashMap<>(inputTailOp.recursiveInputMap);
        this.invertedAnchorInputMap = new HashMap<>(inputTailOp.invertedAnchorInputMap);
        this.invertedRecursiveInputMap = new HashMap<>(inputTailOp.invertedRecursiveInputMap);
        this.fpOp = inputTailOp.fpOp;
        this.headOp = inputTailOp.headOp;
    }

    public Map<LogicalVariable, LogicalVariable> getAnchorInputMap() {
        return Collections.unmodifiableMap(anchorInputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getRecursiveInputMap() {
        return Collections.unmodifiableMap(recursiveInputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getInvertedAnchorInputMap() {
        return Collections.unmodifiableMap(invertedAnchorInputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getInvertedRecursiveInputMap() {
        return Collections.unmodifiableMap(invertedRecursiveInputMap);
    }

    public FixedPointOperator getFixedPointOp() {
        return fpOp;
    }

    public RecursiveHeadOperator getRecursiveHeadOp() {
        return headOp;
    }

    public Collection<LogicalVariable> getPreviousIterationVariables() {
        // We have validated our input at constructor time: we can ignore our recursive input map here.
        return anchorInputMap.values();
    }

    public void replaceInputMaps(Map<LogicalVariable, LogicalVariable> newAnchorInputMap,
            Map<LogicalVariable, LogicalVariable> newRecursiveInputMap) {
        this.anchorInputMap.clear();
        this.recursiveInputMap.clear();
        this.anchorInputMap.putAll(Objects.requireNonNull(newAnchorInputMap));
        this.recursiveInputMap.putAll(Objects.requireNonNull(newRecursiveInputMap));
        if (newAnchorInputMap.values().stream().anyMatch(v -> !newRecursiveInputMap.containsValue(v))) {
            throw new IllegalArgumentException("Input maps should have the exact same output values!");
        }

        // Repopulate our inverted maps.
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> aStream = anchorInputMap.entrySet().stream();
        Stream<Map.Entry<LogicalVariable, LogicalVariable>> rStream = recursiveInputMap.entrySet().stream();
        this.invertedAnchorInputMap = aStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.invertedRecursiveInputMap = rStream.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public static void updateOperatorReferences(RecursiveHeadOperator newHeadOp) {
        List<ILogicalOperator> operatorsInLoop = new ArrayList<>();
        OperatorPropertiesUtil.getOperatorsInLoop(newHeadOp, operatorsInLoop, true);

        // Locate our fixed-point operator...
        FixedPointOperator fpOp = (FixedPointOperator) operatorsInLoop.stream()
                .filter(op -> op.getOperatorTag() == LogicalOperatorTag.FIXED_POINT).findFirst()
                .orElseThrow(() -> new IllegalStateException("FIXED_POINT not found!"));

        // ...and our recursive tail...
        RecursiveTailOperator tailOp = (RecursiveTailOperator) operatorsInLoop.stream()
                .filter(op -> op.getOperatorTag() == LogicalOperatorTag.RECURSIVE_TAIL).findFirst()
                .orElseThrow(() -> new IllegalStateException("RECURSIVE_TAIL not found!"));

        // ...finally, set our references in our tail.
        tailOp.fpOp = fpOp;
        tailOp.headOp = newHeadOp;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // Note: this should be called again after computing the schema for the recursive head.
        schema = new ArrayList<>();
        schema.addAll(getPreviousIterationVariables());
        if (headOp.getSchema() != null) {
            schema.addAll(headOp.getSchema());
        }
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitRecursiveTailOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
                Set<LogicalVariable> visitedSet = new HashSet<>();
                for (LogicalVariable sourceVariable : sources[0]) {
                    if (recursiveInputMap.containsKey(sourceVariable) && !visitedSet.contains(sourceVariable)) {
                        target.addVariable(recursiveInputMap.get(sourceVariable));
                        visitedSet.add(sourceVariable);

                    } else {
                        target.addVariable(sourceVariable);
                    }
                }
            }
        };
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        headOp.computeOutputTypeEnvironment(ctx);
        return ctx.getOutputTypeEnvironment(this);
    }

    // We'll use this method first to get a "base" type environment...
    public IVariableTypeEnvironment computeFirstPassTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ILogicalOperator anchorOp = fpOp.getInputs().get(0).getValue();
        IVariableTypeEnvironment typeEnv = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMissableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL,
                new ITypeEnvPointer[] { () -> ctx.getOutputTypeEnvironment(anchorOp) });
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : anchorInputMap.entrySet()) {
            LogicalVariable toDownstreamVar = entry.getValue();
            LogicalVariable fromAnchorVar = entry.getKey();
            typeEnv.setVarType(toDownstreamVar, typeEnv.getVarType(fromAnchorVar));
        }
        return typeEnv;
    }

    // ...and we'll use this to get our actual type environment.
    public IVariableTypeEnvironment computeSecondPassTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        Mutable<ILogicalOperator> anchorInputOpRef = fpOp.getInputs().get(0);

        // What we produce in-the-loop and before-the-loop might be typed differently.
        IConflictingTypeResolver conflictingTypeResolver = ctx.getConflictingTypeResolver();
        IVariableTypeEnvironment typeEnv = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMissableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL,
                new ITypeEnvPointer[] { new OpRefTypeEnvPointer(anchorInputOpRef, ctx) });
        IVariableTypeEnvironment anchorEnv = ctx.getOutputTypeEnvironment(anchorInputOpRef.getValue());
        IVariableTypeEnvironment headEnv = ctx.getOutputTypeEnvironment(headOp);
        for (LogicalVariable previousVariable : getPreviousIterationVariables()) {
            Object anchorVarType = anchorEnv.getVarType(invertedAnchorInputMap.get(previousVariable));
            Object recursiveVarType = headEnv.getVarType(invertedRecursiveInputMap.get(previousVariable));
            if (Objects.equals(anchorVarType, recursiveVarType)) {
                typeEnv.setVarType(previousVariable, recursiveVarType);
            } else {
                typeEnv.setVarType(previousVariable, conflictingTypeResolver.resolve(anchorVarType, recursiveVarType));
            }
        }
        return typeEnv;
    }

    @Override
    public IVariableTypeEnvironment computeInputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return computeOutputTypeEnvironment(ctx);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.RECURSIVE_TAIL;
    }
}
