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
package org.apache.asterix.graphix.runtime.operator.storage;

import java.util.Collections;

import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;

public class LocalMinimumKResourceFactory extends LSMBTreeLocalResourceFactory {
    private static final long serialVersionUID = 1L;

    public LocalMinimumKResourceFactory(LocalMinimumKComponentProvider componentProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, IVirtualBufferCacheProvider cacheProvider,
            int[] bTreeFields, double bloomFilterFPR) {
        super(componentProvider, typeTraits, comparatorFactories, null, null, null,
                LocalMinimumKOperationTracker.FACTORY, NoOpIOOperationCallbackFactory.INSTANCE,
                NoOpPageWriteCallbackFactory.INSTANCE, AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE,
                cacheProvider, componentProvider, new NoMergePolicyFactory(), Collections.emptyMap(), false,
                bTreeFields, bloomFilterFPR, false, bTreeFields, NoOpCompressorDecompressorFactory.INSTANCE, true,
                TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE, false, false);
    }
}
