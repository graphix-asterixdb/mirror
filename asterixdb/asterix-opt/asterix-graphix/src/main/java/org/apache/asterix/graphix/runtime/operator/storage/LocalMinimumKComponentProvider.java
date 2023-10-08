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

import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousScheduler;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.TransientLocalResourceRepository;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.storage.common.file.ResourceIdFactoryProvider;

public class LocalMinimumKComponentProvider implements IStorageManager, ILSMIOOperationSchedulerProvider {
    private static final long serialVersionUID = 1L;

    private final ILocalResourceRepository localResourceRepository;
    private final IResourceLifecycleManager<IIndex> lifecycleManager;
    private final IResourceIdFactory resourceIdFactory;
    private final ILSMIOOperationScheduler operationScheduler;
    private final IBufferCacheSupplier bufferCacheSupplier;
    private final IIOManager ioManager;

    public LocalMinimumKComponentProvider(IOManager ioManager, IBufferCacheSupplier bufferCacheSupplier,
            long memoryBudget) throws HyracksDataException {
        this.localResourceRepository = new TransientLocalResourceRepository();
        this.resourceIdFactory = new ResourceIdFactoryProvider(localResourceRepository).createResourceIdFactory();
        this.lifecycleManager = new IndexLifecycleManager(memoryBudget);
        this.bufferCacheSupplier = bufferCacheSupplier;
        this.ioManager = ioManager;

        NoOpIoOperationFailedCallback failedCallback = NoOpIoOperationFailedCallback.INSTANCE;
        this.operationScheduler = new SynchronousScheduler(failedCallback);
    }

    @Override
    public IIOManager getIoManager(INCServiceContext ctx) {
        return ioManager;
    }

    @Override
    public IBufferCache getBufferCache(INCServiceContext ctx) {
        return bufferCacheSupplier.get(ctx);
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
        return localResourceRepository;
    }

    @Override
    public IResourceIdFactory getResourceIdFactory(INCServiceContext ctx) {
        return resourceIdFactory;
    }

    @Override
    public IResourceLifecycleManager<IIndex> getLifecycleManager(INCServiceContext ctx) {
        return lifecycleManager;
    }

    @Override
    public ILSMIOOperationScheduler getIoScheduler(INCServiceContext ctx) {
        return operationScheduler;
    }
}
