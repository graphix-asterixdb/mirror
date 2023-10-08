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
package org.apache.asterix.graphix.algebra.operator.physical;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.graphix.runtime.operator.LocalDistinctRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.LocalMinimumKRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.LocalMinimumRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.control.nc.application.NCServiceContext;

public class PathSemanticsReductionPOperator extends AbstractPhysicalOperator {
    // TODO (GLENN): We should have better control over the index and delete memory in the future.
    private static final double INDEX_TO_DELETE_MEMORY_RATIO = 0.75;

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        PathSemanticsReductionOperator semanticsOp = (PathSemanticsReductionOperator) op;
        if (semanticsOp.getWeightExpr() == null) {
            // VirtualFreePageManager requires at least 2 pages... (so, we need one more for us).
            localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(3);

        } else {
            // ...unless we plan on performing deletions, where we'll need at least twice as much space.
            localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(3 * 2);
        }
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.LOCAL_TOP_K;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        PathSemanticsReductionOperator semanticsOp = (PathSemanticsReductionOperator) op;
        Stream<Mutable<ILogicalExpression>> destStream = semanticsOp.getDestDistinctExprList().stream();
        Stream<Mutable<ILogicalExpression>> sourceStream = semanticsOp.getSourceDistinctExprList().stream();
        StructuralPropertiesVector propertiesVector =
                new StructuralPropertiesVector(
                        UnorderedPartitionedProperty.of(Stream.concat(destStream, sourceStream)
                                .filter(e -> e.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE)
                                .map(e -> ((VariableReferenceExpression) e.getValue()).getVariableReference())
                                .collect(Collectors.toSet()), context.getComputationNodeDomain()),
                        new ArrayList<>());
        StructuralPropertiesVector[] required = new StructuralPropertiesVector[] { propertiesVector };
        return new PhysicalRequirements(required, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        // We deliver the same properties as our child.
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        PathSemanticsReductionOperator semanticsOp = (PathSemanticsReductionOperator) op;
        int numExprs = semanticsOp.getExpressions().size();

        // Build our expression evaluators to DISTINCT on...
        IExpressionRuntimeProvider exprRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory[] exprEvalFactories = new IScalarEvaluatorFactory[numExprs];
        for (int i = 0; i < numExprs; i++) {
            ILogicalExpression distinctExpr = semanticsOp.getExpressions().get(i).getValue();
            exprEvalFactories[i] = exprRuntimeProvider.createEvaluatorFactory(distinctExpr,
                    context.getTypeEnvironment(op), inputSchemas, context);
        }

        // ...and our type traits + comparator factories.
        ITypeTraitProvider typeTraitProvider = context.getTypeTraitProvider();
        IBinaryComparatorFactoryProvider comparatorFactoryProvider = context.getBinaryComparatorFactoryProvider();
        ITypeTraits[] typeTraits = new ITypeTraits[numExprs];
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numExprs];
        for (int i = 0; i < numExprs; i++) {
            ILogicalExpression distinctExpr = semanticsOp.getExpressions().get(i).getValue();
            IAType exprType = (IAType) context.getType(distinctExpr, context.getTypeEnvironment(op));
            typeTraits[i] = typeTraitProvider.getTypeTrait(exprType);
            comparatorFactories[i] = comparatorFactoryProvider.getBinaryComparatorFactory(exprType, true);
        }

        // If we have CHEAPEST_PATH semantics, then we need to build factories for our weight expression.
        IScalarEvaluatorFactory weightEvalFactory = null;
        IBinaryComparatorFactory weightComparatorFactory = null;
        ITypeTraits weightTypeTraits = null;
        if (semanticsOp.getWeightExpr() != null) {
            ILogicalExpression weightExpr = semanticsOp.getWeightExpr().getValue();
            IAType exprType = (IAType) context.getType(weightExpr, context.getTypeEnvironment(op));
            weightTypeTraits = typeTraitProvider.getTypeTrait(exprType);
            weightComparatorFactory = comparatorFactoryProvider.getBinaryComparatorFactory(exprType, true);
            weightEvalFactory = exprRuntimeProvider.createEvaluatorFactory(weightExpr, context.getTypeEnvironment(op),
                    inputSchemas, context);
        }

        // Determine the memory division for our index and delete-index components.
        int framesForStandardComponent = 0;
        int framesForDeleteComponent = 0;
        if (semanticsOp.getWeightExpr() != null) {
            int memoryBudgetInFrames = getLocalMemoryRequirements().getMemoryBudgetInFrames();
            framesForStandardComponent = (int) (memoryBudgetInFrames * INDEX_TO_DELETE_MEMORY_RATIO);
            framesForDeleteComponent = memoryBudgetInFrames - framesForStandardComponent;
        }

        // Finally, build our runtime factory.
        IBufferCacheSupplier bufferCacheSupplier = (ctx) -> {
            NCServiceContext serviceContext = (NCServiceContext) ctx;
            INcApplicationContext applicationContext = (INcApplicationContext) serviceContext.getApplicationContext();
            return applicationContext.getBufferCache();
        };
        IPushRuntimeFactory runtimeFactory;
        if (semanticsOp.getWeightExpr() == null && semanticsOp.getKValue() == 1) {
            // TODO (GLENN): We should push down our PROJECTs into these operators.
            runtimeFactory = new LocalDistinctRuntimeFactory(null, 0.01, context.getFrameSize(),
                    getLocalMemoryRequirements().getMemoryBudgetInFrames(), typeTraits, exprEvalFactories,
                    comparatorFactories, bufferCacheSupplier);

        } else if (semanticsOp.getWeightExpr() != null && semanticsOp.getKValue() == 1) {
            runtimeFactory =
                    new LocalMinimumRuntimeFactory(null, 0.01, context.getFrameSize(), framesForStandardComponent,
                            framesForDeleteComponent, typeTraits, weightTypeTraits, exprEvalFactories,
                            weightEvalFactory, comparatorFactories, weightComparatorFactory, bufferCacheSupplier);

        } else if (semanticsOp.getWeightExpr() != null) {
            runtimeFactory = new LocalMinimumKRuntimeFactory(null, 0.01, context.getFrameSize(),
                    framesForStandardComponent, framesForDeleteComponent, typeTraits, weightTypeTraits,
                    exprEvalFactories, weightEvalFactory, comparatorFactories, weightComparatorFactory,
                    bufferCacheSupplier, semanticsOp.getKValue());
        } else {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "FIRST_K_PATHS not implemented!");
        }

        // Contribute our runtime.
        runtimeFactory.setSourceLocation(op.getSourceLocation());
        IVariableTypeEnvironment typeEnvironment = context.getTypeEnvironment(op);
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(typeEnvironment, propagatedSchema, context);
        builder.contributeMicroOperator(op, runtimeFactory, recDesc);
        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(inputOp, 0, op, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
