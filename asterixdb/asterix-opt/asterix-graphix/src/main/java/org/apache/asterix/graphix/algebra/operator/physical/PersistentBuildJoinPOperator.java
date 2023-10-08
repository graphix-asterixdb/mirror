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

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.PersistentBuildJoinOperatorDescriptor;

public class PersistentBuildJoinPOperator extends HybridHashJoinPOperator {
    private final byte markerDesignation;

    public PersistentBuildJoinPOperator(AbstractBinaryJoinOperator.JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int maxInputSizeInFrames, int aveRecordsPerFrame, double fudgeFactor, byte markerDesignation) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities, maxInputSizeInFrames,
                aveRecordsPerFrame, fudgeFactor);
        this.markerDesignation = markerDesignation;
    }

    @Override
    protected IOperatorDescriptor generateOptimizedHashJoinRuntime(JobGenContext context,
            AbstractBinaryJoinOperator joinOp, IOperatorSchema[] inputSchemas, int[] keysLeft, int[] keysRight,
            IBinaryHashFunctionFamily[] leftHashFunFamilies, IBinaryHashFunctionFamily[] rightHashFunFamilies,
            ITuplePairComparatorFactory comparatorFactory, ITuplePairComparatorFactory reverseComparatorFactory,
            IPredicateEvaluatorFactory leftPredEvalFactory, IPredicateEvaluatorFactory rightPredEvalFactory,
            RecordDescriptor recDescriptor, IOperatorDescriptorRegistry spec) throws AlgebricksException {
        int memSizeInFrames = localMemoryRequirements.getMemoryBudgetInFrames();
        if (kind != AbstractBinaryJoinOperator.JoinKind.INNER) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "PBJ cannot be a LEFT-OUTER-JOIN!");
        }
        try {
            return new PersistentBuildJoinOperatorDescriptor(spec, memSizeInFrames, maxInputBuildSizeInFrames,
                    getFudgeFactor(), keysLeft, keysRight, leftHashFunFamilies, rightHashFunFamilies, recDescriptor,
                    comparatorFactory, reverseComparatorFactory, leftPredEvalFactory, rightPredEvalFactory,
                    markerDesignation, false, null);
        } catch (HyracksDataException e) {
            throw AlgebricksException.create(ErrorCode.INSUFFICIENT_MEMORY, e.getMessage());
        }
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PERSISTENT_BUILD_JOIN;
    }
}
