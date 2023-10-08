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

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;

public class LocalMinimumKFileEnvironment implements Serializable {
    private static final long serialVersionUID = 1L;

    private final IIndexBuilderFactory indexBuilderFactory;
    private final IIndexDataflowHelper indexDataflowHelper;
    private final FileReference resourceFile;

    public static class Builder implements Serializable {
        private static final long serialVersionUID = 1L;

        private final IHyracksTaskContext taskContext;

        // We will accumulate the following state on withDirectoryName...
        private FileReference resourceFile;
        private FileSplit[] indexFileSplits;
        private IOManager ioManager;

        // We will accumulate the following state on withMemoryBudget...
        private LocalMinimumKComponentProvider componentProvider;
        private IIndexDataflowHelper indexDataflowHelper;
        private int memoryComponentCachePages;
        private int bufferPageSize;

        // We will accumulate the following state on withResourceTraits, withResourceComparators, etc...
        private ITypeTraits[] resourceTraits;
        private IBinaryComparatorFactory[] resourceComparators;
        private double bloomFilterFPR;
        private int[] bTreeFields;

        public Builder(IHyracksTaskContext taskContext) {
            this.taskContext = Objects.requireNonNull(taskContext);
        }

        public Builder withDirectoryName(String directoryName) throws HyracksDataException {
            // We will place our index files on the first IO device.
            IODeviceHandle ioDeviceHandle = taskContext.getIoManager().getIODevices().get(0);
            this.resourceFile = new FileReference(ioDeviceHandle, directoryName);
            this.indexFileSplits = new FileSplit[] { new FileSplit(null, null) {
                @Override
                public File getFile(IIOManager ioManager) throws HyracksDataException {
                    throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "getFile() should not be called!");
                }

                @Override
                public FileReference getFileReference(IIOManager ioManager) {
                    return resourceFile;
                }
            } };
            this.ioManager = new IOManager(List.of(ioDeviceHandle), new DefaultDeviceResolver(), 1, 10);
            return this;
        }

        public Builder withMemoryBudget(IBufferCacheSupplier bufferCacheSupplier, int bufferPageSize,
                int memoryComponentCachePages) throws HyracksDataException {
            if (resourceFile == null) {
                throw new IllegalStateException("Builder#withDirectoryName must run before this method!");
            }
            INCServiceContext serviceCtx = taskContext.getJobletContext().getServiceContext();
            long memoryBudget = (long) bufferPageSize * memoryComponentCachePages;
            this.componentProvider = new LocalMinimumKComponentProvider(ioManager, bufferCacheSupplier, memoryBudget);
            this.indexDataflowHelper = new IndexDataflowHelper(serviceCtx, componentProvider, resourceFile);
            this.memoryComponentCachePages = memoryComponentCachePages;
            this.bufferPageSize = bufferPageSize;
            return this;
        }

        public Builder withResourceTraits(ITypeTraits[] resourceTraits) {
            this.resourceTraits = Objects.requireNonNull(resourceTraits);
            return this;
        }

        public Builder withResourceComparators(IBinaryComparatorFactory[] resourceComparators) {
            this.resourceComparators = Objects.requireNonNull(resourceComparators);
            return this;
        }

        public Builder withBloomFilterFPR(double bloomFilterFPR) {
            this.bloomFilterFPR = bloomFilterFPR;
            return this;
        }

        public Builder withBTreeFields(int[] bTreeFields) {
            this.bTreeFields = bTreeFields;
            return this;
        }

        public LocalMinimumKFileEnvironment build() throws HyracksDataException {
            ICacheMemoryAllocator cacheAllocator = new HeapBufferAllocator();
            List<IVirtualBufferCache> virtualBufferCaches =
                    List.of(new VirtualBufferCache(cacheAllocator, bufferPageSize, memoryComponentCachePages));
            IResourceFactory resourceFactory = new LocalMinimumKResourceFactory(componentProvider, resourceTraits,
                    resourceComparators, (ctx1, fileRef) -> virtualBufferCaches, bTreeFields, bloomFilterFPR);
            IIndexBuilderFactory indexBuilderFactory = new IndexBuilderFactory(componentProvider,
                    new ConstantFileSplitProvider(indexFileSplits), resourceFactory, false);
            return new LocalMinimumKFileEnvironment(indexBuilderFactory, indexDataflowHelper, resourceFile);
        }
    }

    private LocalMinimumKFileEnvironment(IIndexBuilderFactory indexBuilderFactory,
            IIndexDataflowHelper indexDataflowHelper, FileReference resourceFile) {
        this.indexBuilderFactory = Objects.requireNonNull(indexBuilderFactory);
        this.indexDataflowHelper = Objects.requireNonNull(indexDataflowHelper);
        this.resourceFile = Objects.requireNonNull(resourceFile);
    }

    public IIndexBuilderFactory getIndexBuilderFactory() {
        return indexBuilderFactory;
    }

    public IIndexDataflowHelper getIndexDataflowHelper() {
        return indexDataflowHelper;
    }

    public FileReference getResourceFile() {
        return resourceFile;
    }
}
