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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;

public class TreeIndexDiskOrderScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final ISearchOperationCallbackFactory searchCallbackFactory;

    public TreeIndexDiskOrderScanOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            IIndexDataflowHelperFactory indexHelperFactory, ISearchOperationCallbackFactory searchCallbackFactory) {
        super(spec, 0, 1);
        //TODO(maybe don't fix)
        this.indexHelperFactory = indexHelperFactory;
        this.searchCallbackFactory = searchCallbackFactory;
        this.outRecDescs[0] = outRecDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new TreeIndexDiskOrderScanOperatorNodePushable(ctx, partition, indexHelperFactory,
                searchCallbackFactory);
    }
}
