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

package org.apache.hyracks.tests.am.lsm.rtree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterLocalResourceFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.tests.am.common.LSMTreeOperatorTestHelper;

public class LSMRTreeWithAntiMatterTuplesOperatorTestHelper extends LSMTreeOperatorTestHelper {

    public static final boolean IS_POINT_MBR = false;
    public static final boolean DURABLE = true;

    public LSMRTreeWithAntiMatterTuplesOperatorTestHelper(IOManager ioManager) {
        super(ioManager);
    }

    public IResourceFactory getSecondaryLocalResourceFactory(IStorageManager storageManager,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory,
            int[] btreeFields, ITypeTraits[] secondaryTypeTraits,
            IBinaryComparatorFactory[] secondaryComparatorFactories, IMetadataPageManagerFactory pageManagerFactory) {
        return new LSMRTreeWithAntiMatterLocalResourceFactory(storageManager, secondaryTypeTraits,
                secondaryComparatorFactories, null, null, null, ThreadCountingOperationTrackerFactory.INSTANCE,
                NoOpIOOperationCallbackFactory.INSTANCE, NoOpPageWriteCallbackFactory.INSTANCE, pageManagerFactory,
                getVirtualBufferCacheProvider(), SynchronousSchedulerProvider.INSTANCE, MERGE_POLICY_FACTORY,
                MERGE_POLICY_PROPERTIES, DURABLE, valueProviderFactories, rtreePolicyType, linearizerCmpFactory,
                btreeFields, IS_POINT_MBR, btreeComparatorFactories, null, null);
    }
}
