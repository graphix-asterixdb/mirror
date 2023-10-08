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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
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
 * For some field(s) in a tuple, return true if <i>we</i> have not seen this value {@code k} times before OR if the
 * weight associated with these field(s) is lower than any of the other {@code k} values we have already seen.
 * <ol>
 *   <li>To keep track of whether or not we have seen these values before, we implement a K-priority queue using an
 *   LSM B-tree of a single component.</li>
 *   <li>An element in our priority queue consists of three parts:<ol>
 *     <li>The sort key (consists of one or more fields).</li>
 *     <li>The weight value.</li>
 *     <li>An incrementing counter value to make our element (tuple) unique.</li>
 *   </ol></li>
 *   <li>For each tuple in a frame, we will:<ol>
 *     <li>Check if there exists any values in our index that match the sort key. If we find no such value, we yield
 *     this tuple and store a tuple of {@code <<SORT KEY>, WEIGHT VALUE, 1>>} in our index.</li>
 *     <li>If we find a matching value, then we will advance our index cursor and keep track of the maximum weight
 *     value (and corresponding tuple) we find. If we exhaust our cursor less than {@code k} times, then we yield this
 *     tuple and store a tuple of {@code <<SORT KEY>, WEIGHT VALUE, (number of matching entries in our index + 1)}.</li>
 *     <li>If we find a matching value and the number of matching entries exceeds {@code k} <i>but</i> the maximum
 *     weight value is greater than the weight of our working tuple, then we'll a) issue a delete for the
 *     maximum-value-holding tuple in our index, b) insert the new tuple, and c) yield our working tuple.</li>
 *     <li>If none of the conditions above hold, then do not forward this tuple.</li>
 *   </ol></li>
 * </ol>
 */
public class LocalMinimumKRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    // We have two field-sets of interest: our sort keys, and our weights.
    protected final List<LocalMinimumKFieldContext> sortKeyFields;
    protected final LocalMinimumKFieldContext weightField;
    protected final int indexOfWeightField;
    protected final int indexOfCounterField;
    protected final double bloomFilterFPR;
    protected final long k;

    // The following pertains to the memory management around our index.
    protected final IBufferCacheSupplier bufferCacheSupplier;
    protected final int deleteComponentCachePages;
    protected final int memoryComponentCachePages;
    protected final int bufferPageSize;

    public LocalMinimumKRuntimeFactory(int[] projectionList, double bloomFilterFPR, int bufferPageSize,
            int memoryComponentCachePages, int deleteComponentCachePages, ITypeTraits[] typeTraits,
            ITypeTraits weightTypeTraits, IScalarEvaluatorFactory[] evaluatorFactories,
            IScalarEvaluatorFactory weightEvalFactory, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryComparatorFactory weightComparatorFactory, IBufferCacheSupplier bufferCacheSupplier, long kValue) {
        super(projectionList);
        this.bloomFilterFPR = bloomFilterFPR;
        this.bufferPageSize = bufferPageSize;
        this.memoryComponentCachePages = memoryComponentCachePages;
        this.deleteComponentCachePages = deleteComponentCachePages;
        this.bufferCacheSupplier = bufferCacheSupplier;
        this.k = kValue;
        this.indexOfWeightField = evaluatorFactories.length;
        this.indexOfCounterField = evaluatorFactories.length + 1;
        this.weightField = new LocalMinimumKFieldContext(weightEvalFactory, weightComparatorFactory, weightTypeTraits);
        this.sortKeyFields = IntStream.range(0, evaluatorFactories.length).mapToObj(
                i -> new LocalMinimumKFieldContext(evaluatorFactories[i], comparatorFactories[i], typeTraits[i]))
                .collect(Collectors.toList());
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx)
            throws HyracksDataException {
        // Set up the comparator factories and type traits for our index resource. We need to add a long.
        final IBinaryComparatorFactory[] resourceComparators = new IBinaryComparatorFactory[sortKeyFields.size() + 2];
        final ITypeTraits[] resourceTraits = new ITypeTraits[sortKeyFields.size() + 2];
        for (int i = 0; i < sortKeyFields.size(); i++) {
            resourceComparators[i] = sortKeyFields.get(i).comparatorFactory;
            resourceTraits[i] = sortKeyFields.get(i).typeTraits;
        }
        resourceComparators[indexOfWeightField] = weightField.comparatorFactory;
        resourceComparators[indexOfCounterField] = LongBinaryComparatorFactory.INSTANCE;
        resourceTraits[indexOfWeightField] = weightField.typeTraits;
        resourceTraits[indexOfCounterField] = LongPointable.TYPE_TRAITS;

        // Set our file environments for our matter...
        final String randomNumericString = RandomStringUtils.randomNumeric(10);
        final LocalMinimumKFileEnvironment indexFileEnv = new LocalMinimumKFileEnvironment.Builder(ctx)
                .withDirectoryName(FileUtil.joinPath("lmr", randomNumericString))
                .withMemoryBudget(bufferCacheSupplier, bufferPageSize, memoryComponentCachePages)
                .withResourceTraits(resourceTraits).withResourceComparators(resourceComparators)
                .withBloomFilterFPR(bloomFilterFPR).withBTreeFields(IntStream.range(0, sortKeyFields.size()).toArray())
                .build();

        // ...and anti-matter resources.
        final LocalMinimumKFileEnvironment deleteFileEnv = new LocalMinimumKFileEnvironment.Builder(ctx)
                .withDirectoryName(FileUtil.joinPath("lmr", randomNumericString, "delete"))
                .withMemoryBudget(bufferCacheSupplier, bufferPageSize, deleteComponentCachePages)
                .withResourceTraits(resourceTraits).withResourceComparators(resourceComparators)
                .withBloomFilterFPR(bloomFilterFPR)
                .withBTreeFields(IntStream.range(0, sortKeyFields.size() + 2).toArray()).build();

        // Build our evaluators for our frame...
        final EvaluatorContext evalContext = new EvaluatorContext(ctx);
        final List<IScalarEvaluator> sortKeyEvaluators = new ArrayList<>(sortKeyFields.size());
        for (LocalMinimumKFieldContext sortKeyField : sortKeyFields) {
            sortKeyEvaluators.add(sortKeyField.scalarEvaluatorFactory.createScalarEvaluator(evalContext));
        }
        final IScalarEvaluator weightEvaluator = weightField.scalarEvaluatorFactory.createScalarEvaluator(evalContext);

        // ...and the comparators for our index.
        final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[sortKeyFields.size()];
        for (int i = 0; i < sortKeyFields.size(); i++) {
            LocalMinimumKFieldContext sortKeyField = sortKeyFields.get(i);
            comparatorFactories[i] = sortKeyField.comparatorFactory;
        }

        // Build our push runtime.
        return new AbstractLocalMinimumKPushRuntime(indexFileEnv, deleteFileEnv, ctx) {
            @Override
            protected void evaluateTuple(int i) throws HyracksDataException {
                tRef.reset(tAccess, i);
                for (int f = 0; f < sortKeyEvaluators.size(); f++) {
                    sortKeyEvaluators.get(f).evaluate(tRef, searchFromFrameTupleReference.getField(f));
                }
                weightEvaluator.evaluate(tRef, weightFromFramePointable);
            }

            @Override
            public void open() throws HyracksDataException {
                super.open();

                // We have the sort key values from our frame, as well as the sort key values from our index...
                final int widthOfIndexTuple = sortKeyFields.size() + 2;
                IPointable[] insertIntoIndexPointables = new IPointable[widthOfIndexTuple];
                IPointable[] searchFromFramePointables = new IPointable[sortKeyFields.size()];
                for (int i = 0; i < sortKeyFields.size(); i++) {
                    IPointable forFrameVoidPointable = new VoidPointable();
                    searchFromFramePointables[i] = forFrameVoidPointable;
                    insertIntoIndexPointables[i] = forFrameVoidPointable;
                }
                searchFromFrameTupleReference = new PointableTupleReference(searchFromFramePointables);

                // Finish the pointables for our "insert-into-index" tuple-reference.
                insertIntoIndexPointables[indexOfWeightField] = weightFromFramePointable;
                insertIntoIndexPointables[indexOfCounterField] = kFromFramePointable;
                insertIntoIndexTupleReference = new PointableTupleReference(insertIntoIndexPointables);
                fromIndexTupleReference = PointableTupleReference.create(widthOfIndexTuple, VoidPointable::new);

                // Setup our search predicates.
                MultiComparator indexSearchComparator = MultiComparator.create(comparatorFactories);
                MultiComparator deleteSearchComparator = MultiComparator.create(resourceComparators);
                indexSearchPredicate = new RangePredicate();
                indexSearchPredicate.setLowKeyComparator(indexSearchComparator);
                indexSearchPredicate.setHighKeyComparator(indexSearchComparator);
                indexSearchPredicate.setLowKey(searchFromFrameTupleReference);
                indexSearchPredicate.setHighKey(searchFromFrameTupleReference);
                deleteSearchPredicate = new RangePredicate();
                deleteSearchPredicate.setLowKeyComparator(deleteSearchComparator);
                deleteSearchPredicate.setHighKeyComparator(deleteSearchComparator);
            }
        };
    }

    protected abstract class AbstractLocalMinimumKPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
        // The following pertains to tuples in our frame...
        protected PointableTupleReference searchFromFrameTupleReference;
        protected PointableTupleReference insertIntoIndexTupleReference;
        protected IBinaryComparator weightComparator;
        protected VoidPointable weightFromFramePointable;
        protected LongPointable kFromFramePointable;

        // ...while the following pertains to tuples in our index.
        protected ILSMIndexAccessor indexAccessor;
        protected ILSMIndexAccessor deleteAccessor;
        protected ILSMIndexCursor indexCursor;
        protected ILSMIndexCursor deleteCursor;
        protected RangePredicate indexSearchPredicate;
        protected RangePredicate deleteSearchPredicate;
        protected VoidPointable weightFromIndexPointable;
        protected PointableTupleReference fromIndexTupleReference;

        // We use the following to build / manage our index.
        private final LocalMinimumKFileEnvironment indexFileEnv;
        private final LocalMinimumKFileEnvironment deleteFileEnv;
        private final IHyracksTaskContext taskContext;

        protected AbstractLocalMinimumKPushRuntime(LocalMinimumKFileEnvironment indexFileEnv,
                LocalMinimumKFileEnvironment deleteFileEnv, IHyracksTaskContext taskContext) {
            this.indexFileEnv = Objects.requireNonNull(indexFileEnv);
            this.taskContext = Objects.requireNonNull(taskContext);
            this.deleteFileEnv = Objects.requireNonNull(deleteFileEnv);
        }

        @Override
        public void open() throws HyracksDataException {
            IIndexCursorStats cursorStats = NoOpIndexCursorStats.INSTANCE;
            IIndexAccessParameters accessParameters = IndexAccessParameters.createNoOpParams(cursorStats);
            initAccessAppendRef(taskContext);

            // First, create our indexes...
            IIndexBuilder indexBuilder = indexFileEnv.getIndexBuilderFactory().create(taskContext, 0);
            IIndexBuilder deleteBuilder = deleteFileEnv.getIndexBuilderFactory().create(taskContext, 0);
            indexBuilder.build();
            deleteBuilder.build();

            // ...next, we will open our indexes...
            indexFileEnv.getIndexDataflowHelper().open();
            deleteFileEnv.getIndexDataflowHelper().open();
            IIndex indexInstance = indexFileEnv.getIndexDataflowHelper().getIndexInstance();
            IIndex deleteInstance = deleteFileEnv.getIndexDataflowHelper().getIndexInstance();
            indexAccessor = (ILSMIndexAccessor) indexInstance.createAccessor(accessParameters);
            deleteAccessor = (ILSMIndexAccessor) deleteInstance.createAccessor(accessParameters);
            indexCursor = (ILSMIndexCursor) indexAccessor.createSearchCursor(false);
            deleteCursor = (ILSMIndexCursor) deleteAccessor.createSearchCursor(false);

            // ...finally, we will setup our runtime artifacts.
            weightComparator = weightField.comparatorFactory.createBinaryComparator();
            kFromFramePointable = LongPointable.FACTORY.createPointable(0);
            weightFromFramePointable = new VoidPointable();
            weightFromIndexPointable = new VoidPointable();
            super.open();
        }

        protected abstract void evaluateTuple(int i) throws HyracksDataException;

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tAccess.reset(buffer);
            int tupleCount = tAccess.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                evaluateTuple(i);

                // Search our index for this value.
                indexAccessor.search(indexCursor, indexSearchPredicate);
                try {
                    if (!indexCursor.hasNext()) {
                        // We have not encountered this value before. Pass this down and save this in our index.
                        LOGGER.trace("New value encountered. Saving this in our index.");
                        kFromFramePointable.setLong(1);
                        acceptTupleFromFrame(i);

                    } else {
                        // We have encountered this value before. Exhaust our cursor.
                        long cursorAdvancements = 0;
                        boolean isFirstEncountered = false;
                        for (; indexCursor.hasNext(); cursorAdvancements++) {
                            indexCursor.next();
                            ITupleReference indexTuple = indexCursor.getTuple();
                            byte[] fieldData = indexTuple.getFieldData(indexOfWeightField);
                            int fieldStart = indexTuple.getFieldStart(indexOfWeightField);
                            int fieldLength = indexTuple.getFieldLength(indexOfWeightField);

                            // Make sure that this value does not exist in our delete-component.
                            deleteSearchPredicate.setLowKey(indexTuple);
                            deleteSearchPredicate.setHighKey(indexTuple);
                            deleteAccessor.search(deleteCursor, deleteSearchPredicate);
                            try {
                                if (deleteCursor.hasNext()) {
                                    LOGGER.trace("Value has been previously deleted. Skipping.");
                                    continue;
                                }
                            } finally {
                                deleteCursor.close();
                            }

                            if (!isFirstEncountered) {
                                fromIndexTupleReference.set(indexTuple);
                                weightFromIndexPointable.set(fieldData, fieldStart, fieldLength);
                                isFirstEncountered = true;

                            } else {
                                // Try to find a new maximum value.
                                int c = weightComparator.compare(weightFromIndexPointable.getByteArray(),
                                        weightFromIndexPointable.getStartOffset(), weightFromIndexPointable.getLength(),
                                        fieldData, fieldStart, fieldLength);
                                if (c < 0) {
                                    fromIndexTupleReference.set(indexTuple);
                                    weightFromIndexPointable.set(fieldData, fieldStart, fieldLength);
                                }
                            }
                        }
                        indexCursor.close();

                        // K has not yet exceeded our threshold. Pass this tuple down and update our index.
                        if (cursorAdvancements < k) {
                            LOGGER.trace("Existing value seen under {} encounters.", k);
                            kFromFramePointable.setLong(cursorAdvancements + 1);
                            acceptTupleFromFrame(i);

                        } else {
                            int c = weightComparator.compare(weightFromIndexPointable.getByteArray(),
                                    weightFromIndexPointable.getStartOffset(), weightFromIndexPointable.getLength(),
                                    weightFromFramePointable.getByteArray(), weightFromFramePointable.getStartOffset(),
                                    weightFromFramePointable.getLength());
                            if (c > 0) {
                                LOGGER.trace("Lower weight value found than the maximum from our index.");
                                kFromFramePointable.setLong(cursorAdvancements + 1);
                                acceptTupleFromFrame(i);
                                LOGGER.trace("Now deleting the previous maximum value in our index.");
                                deleteTupleFromIndex(fromIndexTupleReference);
                            }
                        }
                    }

                } finally {
                    indexCursor.close();
                }
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            flushAndReset();
        }

        @Override
        public void close() throws HyracksDataException {
            FileUtils.deleteQuietly(indexFileEnv.getResourceFile().getFile());
            FileUtils.deleteQuietly(deleteFileEnv.getResourceFile().getFile());
            Throwable failure = CleanupUtils.destroy(null, indexCursor);
            failure = CleanupUtils.close(indexFileEnv.getIndexDataflowHelper(), failure);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            failure = CleanupUtils.destroy(null, deleteCursor);
            failure = CleanupUtils.close(deleteFileEnv.getIndexDataflowHelper(), failure);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            super.close();
        }

        private void acceptTupleFromFrame(int i) throws HyracksDataException {
            // Forward the tuple at position i...
            if (projectionList != null) {
                appendProjectionToFrame(i, projectionList);
            } else {
                appendTupleToFrame(i);
            }

            // ...close our cursor (to avoid a deadlock)...
            indexCursor.close();

            // ...and update our index.
            if (!indexAccessor.tryInsert(insertIntoIndexTupleReference)) {
                LOGGER.trace("Buffer is currently full. Flushing this component to disk.");
                indexAccessor.scheduleFlush();
                indexAccessor.insert(insertIntoIndexTupleReference);
                LOGGER.trace("Component has been flushed to disk, and our value has been written.");
            }
        }

        private void deleteTupleFromIndex(ITupleReference deleteTuple) throws HyracksDataException {
            try {
                deleteCursor.close();
                if (!deleteAccessor.tryInsert(deleteTuple)) {
                    LOGGER.trace("Buffer is currently full. Flushing this component to disk.");
                    deleteAccessor.scheduleFlush();
                    deleteAccessor.insert(deleteTuple);
                    LOGGER.trace("Component has been flushed to disk, and our anti-matter value has been written.");
                }

            } catch (HyracksDataException e) {
                // TODO (GLENN): We might not even need this duplicate key error catching...
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY.intValue()) {
                    throw e;
                }
            }
        }
    }

    protected static class LocalMinimumKFieldContext implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final IScalarEvaluatorFactory scalarEvaluatorFactory;
        protected final IBinaryComparatorFactory comparatorFactory;
        protected final ITypeTraits typeTraits;

        protected LocalMinimumKFieldContext(IScalarEvaluatorFactory scalarEvaluatorFactory,
                IBinaryComparatorFactory comparatorFactory, ITypeTraits typeTraits) {
            this.scalarEvaluatorFactory = scalarEvaluatorFactory;
            this.comparatorFactory = comparatorFactory;
            this.typeTraits = typeTraits;
        }
    }
}
