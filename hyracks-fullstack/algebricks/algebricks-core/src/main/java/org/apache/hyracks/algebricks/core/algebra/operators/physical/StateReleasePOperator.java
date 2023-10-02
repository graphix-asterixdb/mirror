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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.message.StateReleaseRuntimeFactory;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.StateReleaseConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.StateReleaseOperatorDescriptor;

public class StateReleasePOperator extends AbstractPhysicalOperator {
    private final IPhysicalOperator physicalDelegateOp;
    private final byte markerDesignation;

    public StateReleasePOperator(IPhysicalOperator physicalDelegateOp, byte markerDesignation) {
        this.physicalDelegateOp = physicalDelegateOp;
        this.markerDesignation = markerDesignation;
    }

    public IPhysicalOperator getPhysicalDelegateOp() {
        return physicalDelegateOp;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        return physicalDelegateOp.getRequiredPropertiesForChildren(op, reqdByParent, context);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        physicalDelegateOp.computeDeliveredProperties(op, context);
    }

    @Override
    public LocalMemoryRequirements getLocalMemoryRequirements() {
        return physicalDelegateOp.getLocalMemoryRequirements();
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        physicalDelegateOp.createLocalMemoryRequirements(op);
    }

    @Override
    public IPhysicalPropertiesVector getDeliveredProperties() {
        return physicalDelegateOp.getDeliveredProperties();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IHyracksJobBuilder stateReleaseJobBuilder = new StateReleaseJobBuilder(builder);
        physicalDelegateOp.contributeRuntimeOperator(stateReleaseJobBuilder, context, op, propagatedSchema,
                inputSchemas, outerPlanSchema);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STATE_RELEASE;
    }

    @Override
    public boolean isMicroOperator() {
        return physicalDelegateOp.isMicroOperator();
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return physicalDelegateOp.expensiveThanMaterialization();
    }

    private class StateReleaseJobBuilder implements IHyracksJobBuilder {
        private final IHyracksJobBuilder delegateBuilder;

        // Callers may invoke multiple calls to job-builder. We need to avoid recreating SR-factories.
        private final Map<IOperatorDescriptor, IOperatorDescriptor> opDescMap = new LinkedHashMap<>();

        public StateReleaseJobBuilder(IHyracksJobBuilder delegateBuilder) {
            this.delegateBuilder = delegateBuilder;
        }

        @Override
        public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime,
                RecordDescriptor recDesc) {
            if (!(runtime instanceof AbstractOneInputOneOutputRuntimeFactory)) {
                throw new IllegalStateException("Cannot use state-release with non-1:1 operators!");
            }
            AbstractOneInputOneOutputRuntimeFactory delegateFactory = (AbstractOneInputOneOutputRuntimeFactory) runtime;
            IPushRuntimeFactory newFactory = new StateReleaseRuntimeFactory(delegateFactory, markerDesignation);
            delegateBuilder.contributeMicroOperator(op, newFactory, recDesc);
        }

        @Override
        public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime, RecordDescriptor recDesc,
                AlgebricksPartitionConstraint pc) {
            if (!(runtime instanceof AbstractOneInputOneOutputRuntimeFactory)) {
                throw new IllegalStateException("Cannot use state-release with non-1:1 operators!");
            }
            AbstractOneInputOneOutputRuntimeFactory delegateFactory = (AbstractOneInputOneOutputRuntimeFactory) runtime;
            IPushRuntimeFactory newFactory = new StateReleaseRuntimeFactory(delegateFactory, markerDesignation);
            delegateBuilder.contributeMicroOperator(op, newFactory, recDesc, pc);
        }

        @Override
        public void contributeHyracksOperator(ILogicalOperator op, IOperatorDescriptor opDesc) {
            if (!(opDesc instanceof AbstractSingleActivityOperatorDescriptor)) {
                throw new IllegalStateException("Cannot use state-release with multiple-activity operators!");
            }
            delegateBuilder.contributeHyracksOperator(op, getReleaseOpDesc(opDesc));
        }

        @Override
        public void contributeGraphEdge(ILogicalOperator src, int srcOutputIndex, ILogicalOperator dest,
                int destInputIndex) {
            delegateBuilder.contributeGraphEdge(src, srcOutputIndex, dest, destInputIndex);
        }

        @Override
        public void contributeConnector(ILogicalOperator exchgOp, IConnectorDescriptor conn) {
            AbstractLogicalOperator logicalOp = (AbstractLogicalOperator) exchgOp;
            StateReleasePOperator releaseOp = (StateReleasePOperator) logicalOp.getPhysicalOperator();
            AbstractExchangePOperator physicalOp = (AbstractExchangePOperator) releaseOp.getPhysicalDelegateOp();
            switch (physicalOp.getOperatorTag()) {
                case ONE_TO_ONE_EXCHANGE:
                    // We do not need to decorate 1:1 connectors.
                    delegateBuilder.contributeConnector(exchgOp, conn);
                    break;

                case HASH_PARTITION_EXCHANGE:
                case BROADCAST_EXCHANGE:
                case HASH_PARTITION_MERGE_EXCHANGE:
                case PARTIAL_BROADCAST_RANGE_FOLLOWING_EXCHANGE:
                case PARTIAL_BROADCAST_RANGE_INTERSECT_EXCHANGE:
                case RANDOM_MERGE_EXCHANGE:
                case RANDOM_PARTITION_EXCHANGE:
                case RANGE_PARTITION_EXCHANGE:
                case RANGE_PARTITION_MERGE_EXCHANGE:
                case SEQUENTIAL_MERGE_EXCHANGE:
                case SORT_MERGE_EXCHANGE:
                    delegateBuilder.contributeConnector(exchgOp,
                            new StateReleaseConnectorDescriptor(delegateBuilder.getJobSpec(), conn, markerDesignation));
                    delegateBuilder.getJobSpec().getConnectorMap().remove(conn.getConnectorId());
                    break;

                default:
                    throw new IllegalStateException("Non-exchange operator given!");
            }
        }

        @Override
        public void contributeConnectorWithTargetConstraint(ILogicalOperator exchgOp, IConnectorDescriptor conn,
                TargetConstraint numberOfTargetPartitions) {
            AbstractLogicalOperator logicalOp = (AbstractLogicalOperator) exchgOp;
            StateReleasePOperator releaseOp = (StateReleasePOperator) logicalOp.getPhysicalOperator();
            AbstractExchangePOperator physicalOp = (AbstractExchangePOperator) releaseOp.getPhysicalDelegateOp();
            switch (physicalOp.getOperatorTag()) {
                case ONE_TO_ONE_EXCHANGE:
                    // We do not need to decorate 1:1 connectors.
                    delegateBuilder.contributeConnectorWithTargetConstraint(exchgOp, conn, numberOfTargetPartitions);
                    break;

                case HASH_PARTITION_EXCHANGE:
                case BROADCAST_EXCHANGE:
                case HASH_PARTITION_MERGE_EXCHANGE:
                case PARTIAL_BROADCAST_RANGE_FOLLOWING_EXCHANGE:
                case PARTIAL_BROADCAST_RANGE_INTERSECT_EXCHANGE:
                case RANDOM_MERGE_EXCHANGE:
                case RANDOM_PARTITION_EXCHANGE:
                case RANGE_PARTITION_EXCHANGE:
                case RANGE_PARTITION_MERGE_EXCHANGE:
                case SEQUENTIAL_MERGE_EXCHANGE:
                case SORT_MERGE_EXCHANGE:
                    delegateBuilder.contributeConnectorWithTargetConstraint(exchgOp,
                            new StateReleaseConnectorDescriptor(delegateBuilder.getJobSpec(), conn, markerDesignation),
                            numberOfTargetPartitions);
                    delegateBuilder.getJobSpec().getConnectorMap().remove(conn.getConnectorId());
                    break;

                default:
                    throw new IllegalStateException("Non-exchange operator given!");
            }
        }

        @Override
        public void contributeAlgebricksPartitionConstraint(IOperatorDescriptor opDesc,
                AlgebricksPartitionConstraint apc) {
            delegateBuilder.contributeAlgebricksPartitionConstraint(getReleaseOpDesc(opDesc), apc);
        }

        @Override
        public JobSpecification getJobSpec() {
            return delegateBuilder.getJobSpec();
        }

        @Override
        public void buildSpec(List<ILogicalOperator> roots) {
            throw new IllegalStateException("Cannot build using this state-release job builder! Use the delegate!");
        }

        private IOperatorDescriptor getReleaseOpDesc(IOperatorDescriptor opDesc) {
            if (opDescMap.containsKey(opDesc)) {
                return opDescMap.get(opDesc);
            }
            JobSpecification spec = delegateBuilder.getJobSpec();
            AbstractSingleActivityOperatorDescriptor delegateOpDesc = (AbstractSingleActivityOperatorDescriptor) opDesc;
            IOperatorDescriptor newOpDesc = new StateReleaseOperatorDescriptor(spec, delegateOpDesc, markerDesignation);
            spec.getOperatorMap().remove(delegateOpDesc.getOperatorId());
            opDescMap.put(opDesc, newOpDesc);
            return newOpDesc;
        }
    }

    @Override
    public String toString() {
        return String.format("STATE_RELEASE (%s)", physicalDelegateOp.toString());
    }

    @Override
    public String toString(boolean verbose) {
        return String.format("STATE_RELEASE (%s)", physicalDelegateOp.toString(verbose));
    }
}
