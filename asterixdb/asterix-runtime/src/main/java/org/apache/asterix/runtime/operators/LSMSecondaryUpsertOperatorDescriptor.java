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
package org.apache.asterix.runtime.operators;

import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

public class LSMSecondaryUpsertOperatorDescriptor extends LSMTreeInsertDeleteOperatorDescriptor {

    private static final long serialVersionUID = 3L;
    private final int[] prevValuePermutation;
    protected final int operationFieldIndex;
    protected final IBinaryIntegerInspectorFactory operationInspectorFactory;
    private final ITupleFilterFactory prevTupleFilterFactory;

    public LSMSecondaryUpsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IIndexDataflowHelperFactory indexHelperFactory,
            ITupleFilterFactory tupleFilterFactory, ITupleFilterFactory prevTupleFilterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory, int operationFieldIndex,
            IBinaryIntegerInspectorFactory operationInspectorFactory, int[] prevValuePermutation,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) {
        super(spec, outRecDesc, fieldPermutation, IndexOperation.UPSERT, indexHelperFactory, tupleFilterFactory, false,
                modificationOpCallbackFactory, tuplePartitionerFactory, partitionsMap);
        this.prevValuePermutation = prevValuePermutation;
        this.operationFieldIndex = operationFieldIndex;
        this.operationInspectorFactory = operationInspectorFactory;
        this.prevTupleFilterFactory = prevTupleFilterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor intputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new LSMSecondaryUpsertOperatorNodePushable(ctx, partition, indexHelperFactory, modCallbackFactory,
                tupleFilterFactory, prevTupleFilterFactory, fieldPermutation, intputRecDesc, operationFieldIndex,
                operationInspectorFactory, prevValuePermutation, tuplePartitionerFactory, partitionsMap);
    }
}
