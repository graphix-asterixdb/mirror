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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.asterix.graphix.runtime.operator.storage.LocalMinimumKFileEnvironment;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.file.FileUtil;

/**
 * A special instance of {@link LocalMinimumKRuntimeFactory} where {@code k} = 1. This allows us to remove the counter
 * attribute from elements in our priority queue (and save on some memory + processing).
 */
public class LocalMinimumRuntimeFactory extends LocalMinimumKRuntimeFactory {
    private static final long serialVersionUID = 1L;

    public LocalMinimumRuntimeFactory(int[] projectionList, double bloomFilterFPR, int bufferPageSize,
            int memoryComponentCachePages, int deleteComponentCachePages, ITypeTraits[] typeTraits,
            ITypeTraits weightTypeTraits, IScalarEvaluatorFactory[] evaluatorFactories,
            IScalarEvaluatorFactory weightEvalFactory, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryComparatorFactory weightComparatorFactory, IBufferCacheSupplier bufferCacheSupplier) {
        super(projectionList, bloomFilterFPR, bufferPageSize, memoryComponentCachePages, deleteComponentCachePages,
                typeTraits, weightTypeTraits, evaluatorFactories, weightEvalFactory, comparatorFactories,
                weightComparatorFactory, bufferCacheSupplier, 1);
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx)
            throws HyracksDataException {
        // Set up the comparator factories and type traits for our index resource.
        final IBinaryComparatorFactory[] resourceComparators = new IBinaryComparatorFactory[sortKeyFields.size() + 1];
        final ITypeTraits[] resourceTraits = new ITypeTraits[sortKeyFields.size() + 1];
        for (int i = 0; i < sortKeyFields.size(); i++) {
            resourceComparators[i] = sortKeyFields.get(i).comparatorFactory;
            resourceTraits[i] = sortKeyFields.get(i).typeTraits;
        }
        resourceComparators[indexOfWeightField] = weightField.comparatorFactory;
        resourceTraits[indexOfWeightField] = weightField.typeTraits;

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
                .withBTreeFields(IntStream.range(0, sortKeyFields.size() + 1).toArray()).build();

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
                final int widthOfIndexTuple = sortKeyFields.size() + 1;
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
}
