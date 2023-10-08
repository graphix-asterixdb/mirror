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
package org.apache.asterix.graphix.runtime.operator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.asterix.graphix.runtime.operator.storage.LocalMinimumKFileEnvironment;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A special instance of {@link LocalMinimumKRuntimeFactory} where {@code k} = 1 and where there exists no weight
 * attribute. This allows us to treat our index as a set rather than an entire priority queue (and save on some
 * memory + processing).
 */
public class LocalDistinctRuntimeFactory extends LocalMinimumKRuntimeFactory {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    public LocalDistinctRuntimeFactory(int[] projectionList, double bloomFilterFPR, int bufferPageSize,
            int memoryComponentCachePages, ITypeTraits[] typeTraits, IScalarEvaluatorFactory[] evaluatorFactories,
            IBinaryComparatorFactory[] comparatorFactories, IBufferCacheSupplier bufferCacheSupplier) {
        super(projectionList, bloomFilterFPR, bufferPageSize, memoryComponentCachePages, 0, typeTraits, null,
                evaluatorFactories, null, comparatorFactories, null, bufferCacheSupplier, 1);
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx)
            throws HyracksDataException {
        final IBinaryComparatorFactory[] resourceComparators = new IBinaryComparatorFactory[sortKeyFields.size()];
        final ITypeTraits[] resourceTraits = new ITypeTraits[sortKeyFields.size()];
        for (int i = 0; i < sortKeyFields.size(); i++) {
            resourceComparators[i] = sortKeyFields.get(i).comparatorFactory;
            resourceTraits[i] = sortKeyFields.get(i).typeTraits;
        }

        // Set the file environment for index.
        final String randomNumericString = RandomStringUtils.randomNumeric(10);
        final LocalMinimumKFileEnvironment matterFileEnv = new LocalMinimumKFileEnvironment.Builder(ctx)
                .withDirectoryName(FileUtil.joinPath("lmr", randomNumericString))
                .withMemoryBudget(bufferCacheSupplier, bufferPageSize, memoryComponentCachePages)
                .withResourceTraits(resourceTraits).withResourceComparators(resourceComparators)
                .withBloomFilterFPR(bloomFilterFPR).withBTreeFields(IntStream.range(0, sortKeyFields.size()).toArray())
                .build();

        // Build our evaluators for our frame...
        final EvaluatorContext evalContext = new EvaluatorContext(ctx);
        final List<IScalarEvaluator> sortKeyEvaluators = new ArrayList<>(sortKeyFields.size());
        for (LocalMinimumKFieldContext sortKeyField : sortKeyFields) {
            sortKeyEvaluators.add(sortKeyField.scalarEvaluatorFactory.createScalarEvaluator(evalContext));
        }

        // ...and the comparators for our index.
        final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[sortKeyFields.size()];
        for (int i = 0; i < sortKeyFields.size(); i++) {
            LocalMinimumKFieldContext sortKeyField = sortKeyFields.get(i);
            comparatorFactories[i] = sortKeyField.comparatorFactory;
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            // The following pertains to tuples in our frame...
            private PointableTupleReference fromFrameTupleReference;

            // ...while the following pertains to tuples in our index.
            private ILSMIndexAccessor matterIndexAccessor;
            private ILSMIndexCursor matterIndexCursor;
            private RangePredicate matterSearchPredicate;

            @Override
            public void open() throws HyracksDataException {
                IIndexCursorStats cursorStats = NoOpIndexCursorStats.INSTANCE;
                IIndexAccessParameters accessParameters = IndexAccessParameters.createNoOpParams(cursorStats);
                initAccessAppendRef(ctx);

                // Create our index...
                IIndexBuilder matterIndexBuilder = matterFileEnv.getIndexBuilderFactory().create(ctx, 0);
                matterIndexBuilder.build();

                // ...and open our index.
                matterFileEnv.getIndexDataflowHelper().open();
                IIndex matterIndex = matterFileEnv.getIndexDataflowHelper().getIndexInstance();
                matterIndexAccessor = (ILSMIndexAccessor) matterIndex.createAccessor(accessParameters);
                matterIndexCursor = (ILSMIndexCursor) matterIndexAccessor.createSearchCursor(false);

                // We have the sort key values from our frame, as well as the sort key values from our index...
                MultiComparator matterSearchComparator = MultiComparator.create(comparatorFactories);
                matterSearchPredicate = new RangePredicate(fromFrameTupleReference, fromFrameTupleReference, true, true,
                        matterSearchComparator, matterSearchComparator);
                IPointable[] searchFromFramePointables = new IPointable[sortKeyFields.size()];
                for (int i = 0; i < sortKeyFields.size(); i++) {
                    IPointable voidPointable = new VoidPointable();
                    searchFromFramePointables[i] = voidPointable;
                }
                fromFrameTupleReference = new PointableTupleReference(searchFromFramePointables);
                super.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int tupleCount = tAccess.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tRef.reset(tAccess, i);
                    for (int f = 0; f < sortKeyEvaluators.size(); f++) {
                        sortKeyEvaluators.get(f).evaluate(tRef, fromFrameTupleReference.getField(f));
                    }

                    // Search our matter index for this value.
                    matterIndexAccessor.search(matterIndexCursor, matterSearchPredicate);
                    try {
                        if (!matterIndexCursor.hasNext()) {
                            // Forward the tuple at position i...
                            LOGGER.trace("New value encountered. Saving this in our index.");
                            if (projectionList != null) {
                                appendProjectionToFrame(i, projectionList);
                            } else {
                                appendTupleToFrame(i);
                            }

                            // ...close our cursor (to avoid a deadlock)...
                            matterIndexCursor.close();

                            // ...and update our index.
                            if (!matterIndexAccessor.tryInsert(fromFrameTupleReference)) {
                                LOGGER.trace("Buffer is currently full. Flushing this component to disk.");
                                matterIndexAccessor.scheduleFlush();
                                matterIndexAccessor.insert(fromFrameTupleReference);
                                LOGGER.trace("Component has been flushed to disk, and our value has been written.");
                            }
                        }
                    } finally {
                        matterIndexCursor.close();
                    }
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                flushAndReset();
            }

            @Override
            public void close() throws HyracksDataException {
                FileUtils.deleteQuietly(matterFileEnv.getResourceFile().getFile());
                Throwable failure = CleanupUtils.destroy(null, matterIndexCursor);
                failure = CleanupUtils.close(matterFileEnv.getIndexDataflowHelper(), failure);
                if (failure != null) {
                    throw HyracksDataException.create(failure);
                }
                super.close();
            }
        };
    }
}
