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
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor;
import org.apache.hyracks.dataflow.std.message.FrameTupleListener;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Operator descriptor for the PBJ (Persistent-Build Join) operator, to be used in conjunction with the
 * {@link FixedPointOperatorDescriptor} operator.
 * <p>
 * A specialized instance of our optimized HHJ (Hybrid Hash Join), where the build phase occurs once (persistent-build)
 * and the <i>entire</i> probe phase occurs repeatedly (transient-probe). The signal used to trigger the
 * spilled-partition JOINing process is a message frame recognized by {@link MarkerMessageConsumer}). On receiving
 * a signal, the following actions occur:
 * <ol>
 *   <li>Prepare a new JOIN object with the build artifacts of the old, and a new set of probe run files.</li>
 *   <li>All remaining probe tuples are JOINed and the remaining results are pushed forward.</li>
 *   <li>We can now begin the spilled-partition JOINing. For each build-probe partition pair, we:</li>
 *   <ol>
 *     <li>Get a delete-on-close reader for our probe, and a <i>non-delete-on-close</i> reader for our build.</li>
 *     <li>Invoke the joining process for our partition pair. TODO (GLENN): For all build-side generated artifacts,
 *     we should (in the future) <b>not</b> delete them and reuse them when possible.</li>
 *   </ol>
 *   <li>With all of the spilled-partition JOINing now finished, we can forget the JOIN object we just used.</li>
 * </ol>
 *
 * @see OptimizedHybridHashJoinOperatorDescriptor
 */
public class PersistentBuildJoinOperatorDescriptor extends OptimizedHybridHashJoinOperatorDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    // We use the following to recognize the appropriate marker frame.
    private final byte markerDesignation;

    // PBJ requires both the memory-size for the JOIN to be halved ( TODO (GLENN): we should compute this better... ).
    public PersistentBuildJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memSizeInFrames, int inputSize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFamily[] propHashFuncFactories,
            IBinaryHashFunctionFamily[] buildHashFuncFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPairCompFactory01, ITuplePairComparatorFactory tupPairCompFactory10,
            IPredicateEvaluatorFactory predEvalFactory0, IPredicateEvaluatorFactory predEvalFactory1,
            byte markerDesignation, boolean isLeftOuter, IMissingWriterFactory[] nonMatchWriterFactories)
            throws HyracksDataException {
        super(spec, (int) (2 + Math.ceil((memSizeInFrames - 2) / 2.0)), inputSize0, factor, keys0, keys1,
                propHashFuncFactories, buildHashFuncFactories, recordDescriptor, tupPairCompFactory01,
                tupPairCompFactory10, predEvalFactory0, predEvalFactory1, isLeftOuter, nonMatchWriterFactories);
        if (memSizeInFrames < 8) {
            throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY);
        }
        this.markerDesignation = markerDesignation;
        this.outRecDescs[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        AbstractActivityNode phase1 = new PersistentBuildActivityNode(buildAid, probeAid);
        AbstractActivityNode phase2 = new TransientProbeActivityNode(probeAid, buildAid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);
        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);
        builder.addBlockingEdge(phase1, phase2);
        builder.addTargetEdge(0, phase2, 0);
    }

    private class PersistentBuildActivityNode extends PartitionAndBuildActivityNode {
        public PersistentBuildActivityNode(ActivityId id, ActivityId probeAid) {
            super(id, probeAid);
        }
    }

    private class TransientProbeActivityNode extends ProbeAndJoinActivityNode {
        public TransientProbeActivityNode(ActivityId id, ActivityId buildAid) {
            super(id, buildAid);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new ProbeAndJoinPushRuntime(ctx, recordDescProvider, partition, nPartitions) {
                private final MarkerMessageConsumer markerConsumer = new MarkerMessageConsumer(markerDesignation);
                private final FrameTupleListener frameTupleListener =
                        new FrameTupleListener(recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0));

                // We need to spawn a new thread to handle our spilled-partition-JOINing.
                private final ExecutorService executor = Executors.newSingleThreadExecutor(Thread::new);
                private final Map<Integer, CompletableFuture<?>> futureMap = new HashMap<>();
                private final AtomicBoolean isFailed = new AtomicBoolean(false);
                private String joinerThreadNamePrefix;

                private void handleExecutorErrors() throws HyracksDataException {
                    Iterator<CompletableFuture<?>> futureIterator = futureMap.values().iterator();
                    try {
                        while (futureIterator.hasNext()) {
                            CompletableFuture<?> future = futureIterator.next();
                            if (future.isDone()) {
                                future.get();
                                futureIterator.remove();
                            }
                        }
                    } catch (ExecutionException | InterruptedException e) {
                        fail();
                        throw HyracksDataException.create(e);
                    }
                }

                private void closeExecutor() throws HyracksDataException {
                    executor.shutdownNow();
                    while (!executor.isTerminated()) {
                        try {
                            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                                for (Map.Entry<Integer, CompletableFuture<?>> futureEntry : futureMap.entrySet()) {
                                    if (!futureEntry.getValue().isDone()) {
                                        String joinerName = "SpilledPartitionJoiner" + futureEntry.getKey();
                                        LOGGER.debug("Waiting for {} to finish.", joinerName);
                                    }
                                }
                            }

                        } catch (InterruptedException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                    handleExecutorErrors();
                }

                @Override
                public void open() throws HyracksDataException {
                    joinerThreadNamePrefix = Thread.currentThread().getName() + "_SpilledPartitionJoinerRun_";

                    // We want to protect our downstream writer from concurrent access.
                    IFrameWriter originalWriter = writer;
                    writer = new IFrameWriter() {
                        private final Lock writerLock = new ReentrantLock();

                        @Override
                        public void open() throws HyracksDataException {
                            originalWriter.open();
                        }

                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                            writerLock.lock();
                            if (!FrameHelper.isMessageFrame(buffer) && !frameTupleListener.acceptFrame(buffer)) {
                                LOGGER.trace("No tuples found on {}. Not forwarding frame.",
                                        Thread.currentThread().getName());
                                writerLock.unlock();
                                return;
                            }
                            originalWriter.nextFrame(buffer);
                            writerLock.unlock();
                        }

                        @Override
                        public void flush() throws HyracksDataException {
                            writerLock.lock();
                            originalWriter.flush();
                            writerLock.unlock();
                        }

                        @Override
                        public void fail() throws HyracksDataException {
                            originalWriter.fail();
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            originalWriter.close();
                        }
                    };
                    super.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (FrameHelper.isMessageFrame(buffer)) {
                        LOGGER.trace("A marker has been given to {}.", Thread.currentThread().getName());
                        if (!markerConsumer.accept(buffer)) {
                            // We have encountered a different marker frame not meant for us. Forward this.
                            writer.nextFrame(buffer);
                            return;
                        }

                        // We eagerly handle our errors from our executor.
                        handleExecutorErrors();

                        // Prepare a new JOIN to handle all probe tuples hereafter.
                        final PartitionAndJoinEnvironment joinEnvironment = createEnvironment();
                        final OptimizedHybridHashJoin newJoin = new PersistentBuildJoin(state.hybridHJ);
                        final OptimizedHybridHashJoin closingJoin = state.hybridHJ;
                        final VSizeFrame markerBuffer = new VSizeFrame(ctx);
                        FrameUtils.copyWholeFrame(buffer, markerBuffer.getBuffer());
                        markerBuffer.getBuffer().position(0);
                        state.hybridHJ = newJoin;

                        // Delegate this spilled-partition-JOINing to a separate thread.
                        final int consumerInvocations = markerConsumer.getInvocations((short) partition);
                        final String newThreadName = joinerThreadNamePrefix + consumerInvocations;
                        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                        futureMap.put(consumerInvocations, completableFuture);
                        executor.submit(() -> {
                            final String originalThreadName = Thread.currentThread().getName();
                            Thread.currentThread().setName(originalThreadName + "_" + newThreadName);
                            LOGGER.trace("{} has started.", newThreadName);
                            try {
                                // Push our probe tuples forward.
                                frameTupleListener.startListening();
                                closingJoin.completeProbe(writer);
                                PersistentBuildJoin.closeSpilledProbePartitions(closingJoin);

                                // Join all partition-pairs according to our optimized HHJ policy.
                                joinEnvironment.rPartbuff.reset();
                                BitSet partitionStatus = state.hybridHJ.getPartitionStatus();
                                for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid =
                                        partitionStatus.nextSetBit(pid + 1)) {
                                    RunFileReader bReader = newJoin.getBuildRFReader(pid);
                                    RunFileReader pReader = closingJoin.getProbeRFReader(pid);
                                    int bSize = closingJoin.getBuildPartitionSizeInTup(pid);
                                    int pSize = closingJoin.getProbePartitionSizeInTup(pid);
                                    if (pSize == 0 || bSize == 0) {
                                        if (isLeftOuter && pReader != null) {
                                            appendNullToProbeTuples(pReader, joinEnvironment);
                                        }
                                        if (bReader != null) {
                                            bReader.close();
                                        }
                                        if (pReader != null) {
                                            pReader.close();
                                        }
                                        LOGGER.trace("Skipping JOIN for partition {}.", pid);
                                        continue;
                                    }
                                    LOGGER.trace("Starting JOIN for partition {}.", pid);
                                    joinPartitionPair(bReader, pReader, bSize, pSize, joinEnvironment, 1);
                                }

                                // After joining all partition pairs, propagate our marker frame.
                                writer.flush();
                                if (frameTupleListener.hasTuples()) {
                                    LOGGER.trace("{} has {} tuples. Marker flag has been raised.", newThreadName,
                                            frameTupleListener.getNumberOfTuples());
                                    MarkerMessageConsumer.raiseLiveFlag(markerBuffer.getBuffer());
                                }
                                frameTupleListener.stopListening();
                                writer.nextFrame(markerBuffer.getBuffer());
                                ctx.deallocateFrames(ctx.getInitialFrameSize());
                                LOGGER.trace("{} has finished.", newThreadName);
                                completableFuture.complete(null);

                            } catch (Exception e) {
                                completableFuture.completeExceptionally(e);

                            } finally {
                                Thread.currentThread().setName(originalThreadName);
                            }
                        });
                        LOGGER.trace("{} has been submitted to the work queue.", newThreadName);

                    } else {
                        super.nextFrame(buffer);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    LOGGER.debug("close() has been invoked for this runtime!");
                    closeExecutor();
                    super.close();
                    LOGGER.debug("close() has finished for this runtime!");
                }

                @Override
                public void fail() throws HyracksDataException {
                    if (!isFailed.get()) {
                        super.fail();

                        // On fail, we still need to wait for our executor-service to close all of our threads.
                        isFailed.getAndSet(true);
                        closeExecutor();
                    }
                }
            };
        }
    }
}
