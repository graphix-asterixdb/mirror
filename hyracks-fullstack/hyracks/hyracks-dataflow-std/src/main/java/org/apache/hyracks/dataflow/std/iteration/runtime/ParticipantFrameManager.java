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
package org.apache.hyracks.dataflow.std.iteration.runtime;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FixedSizeFrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.iteration.FixedPointLoggingException;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.hyracks.dataflow.std.iteration.common.TLock;
import org.apache.hyracks.dataflow.std.iteration.common.Token;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.hyracks.dataflow.std.message.producer.MarkerMessageProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ParticipantFrameManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_FREQUENCY_SECONDS = 1;

    private MarkerMessageProducer markerProducer;
    private MarkerMessageConsumer markerConsumer;
    private IFrameWriter downstreamWriter;
    private IFrame downstreamBuffer;
    private boolean wasCloseCalled;
    private int partition;

    // We will inform our participant of results through this queue.
    private BlockingQueue<Message> messageQueue;
    private int receiptTimestamp = 0;

    // We use the following to buffer tuples in between stages.
    private BlockingQueue<ByteBuffer> cleanBufferFrames;
    private BlockingQueue<ByteBuffer> dirtyBufferFrames;
    private FixedSizeFrame activeBufferFrame;
    private ReentrantLock captureLock;

    // If we exceed our buffer in between stages, we will need to write to disk.
    private FrameTupleAccessor frameTupleAccessor;
    private FrameTupleAppender frameTupleAppender;
    private RunFileReader runFileReader;
    private RunFileWriter runFileWriter;
    private IHyracksTaskContext ctx;

    // To prevent deadlocks, our release action must be separate from our consume action.
    private CompletableFuture<?> releaseProcessorFuture;
    private BlockingQueue<Token> tokenQueue;

    public void initialize(IFrameWriter downstreamWriter, byte consumerDesignation, int partition,
            BlockingQueue<Message> toParticipantQueue, BlockingQueue<Token> toFrameManageQueue,
            FrameTupleAccessor frameTupleAccessor, int bufferSizePages, IHyracksTaskContext ctx)
            throws HyracksDataException {
        this.markerProducer = new MarkerMessageProducer(consumerDesignation);
        this.markerConsumer = new MarkerMessageConsumer(consumerDesignation);
        this.downstreamWriter = Objects.requireNonNull(downstreamWriter);
        this.messageQueue = Objects.requireNonNull(toParticipantQueue);
        this.ctx = Objects.requireNonNull(ctx);
        this.wasCloseCalled = false;
        this.partition = partition;

        // Reserve our buffer memory here...
        this.cleanBufferFrames = new LinkedBlockingQueue<>(bufferSizePages);
        this.dirtyBufferFrames = new LinkedBlockingQueue<>(bufferSizePages);
        for (int i = 0; i < bufferSizePages - 1; i++) {
            this.cleanBufferFrames.add(ctx.allocateFrame());
        }
        this.downstreamBuffer = new VSizeFrame(ctx);
        this.activeBufferFrame = new FixedSizeFrame(ctx.allocateFrame());
        this.frameTupleAppender = new FixedSizeFrameTupleAppender();
        this.frameTupleAppender.reset(activeBufferFrame, true);
        this.frameTupleAccessor = Objects.requireNonNull(frameTupleAccessor);
        this.captureLock = TLock.get("CaptureResources_" + partition);

        // Spawn our release processor.
        this.tokenQueue = Objects.requireNonNull(toFrameManageQueue);
        this.releaseProcessorFuture = new CompletableFuture<>();
        Executors.newSingleThreadExecutor(Thread::new).submit(new ReleaseFromBufferProcessor(Thread.currentThread()));
    }

    public void accept(ByteBuffer buffer) throws HyracksDataException {
        // Our normal operation: we will buffer normal data frames.
        if (!FrameHelper.isMessageFrame(buffer)) {
            frameTupleAccessor.reset(buffer);

            // We want to respect the IFrameWriter contract here: do not empty frames (after close).
            if (wasCloseCalled) {
                if (frameTupleAccessor.getTupleCount() == 0) {
                    LOGGER.warn("Frame with no tuples received after close. Dropping this frame.");
                    return;
                }
                throw new FixedPointLoggingException("Frame with tuples received after close!");
            }
            if (frameTupleAccessor.getTupleCount() > 0) {
                LOGGER.trace("Performing buffer operation on partition {}.", partition);
                captureLock.lock();
                capture();
                captureLock.unlock();
            }
            return;
        }

        // We should only be working with state-release markers here (otherwise, we will incur a deadlock).
        LOGGER.trace("A message has been received on partition {}.", partition);
        if (!markerConsumer.accept(buffer)) {
            throw new FixedPointLoggingException("Illegal message received on partition " + partition + "!");
        }

        // Inform our election participant.
        try {
            short lastAcceptedProducer = markerConsumer.getLastAcceptedProducer();
            LOGGER.trace("Marker received from {} on partition {}.", lastAcceptedProducer, partition);
            Message electionParticipantMessage = new Message(lastAcceptedProducer, partition, Event.MARKER,
                    ++receiptTimestamp, markerConsumer.isLiveFlagRaised());
            messageQueue.put(electionParticipantMessage);

        } catch (InterruptedException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void capture() throws HyracksDataException {
        for (int t = 0; t < frameTupleAccessor.getTupleCount(); t++) {
            // Part #1: can we fit this into our current buffer?
            LOGGER.trace("Putting tuple {} in memory-buffer on partition {}.", t, partition);
            if (frameTupleAppender.append(frameTupleAccessor, t)) {
                continue;
            }

            // Part #2: can we fit these tuples into a new buffer?
            LOGGER.trace("Could not fit tuple into working buffer. Now trying a new buffer.");
            if (!cleanBufferFrames.isEmpty()) {
                try {
                    dirtyBufferFrames.put(frameTupleAppender.getBuffer());
                    activeBufferFrame.reset(cleanBufferFrames.take());
                    frameTupleAppender.reset(activeBufferFrame, true);

                } catch (InterruptedException e) {
                    throw HyracksDataException.create(e);
                }
                if (!frameTupleAppender.append(frameTupleAccessor, t)) {
                    throw new HyracksDataException(ErrorCode.INSUFFICIENT_MEMORY);
                }
                continue;
            }

            // Part #3: spill these tuples to our run file.
            LOGGER.trace("Buffer is full. Spilling current buffer frame to disk.");
            if (runFileWriter == null) {
                String runFilePrefix = ParticipantFrameManager.class.getSimpleName();
                FileReference runFile = ctx.getJobletContext().createManagedWorkspaceFile(runFilePrefix);
                runFileWriter = new RunFileWriter(runFile, ctx.getIoManager());
                runFileWriter.open();
            }
            runFileWriter.nextFrame(frameTupleAppender.getBuffer());
            frameTupleAppender.reset(activeBufferFrame, true);
            t--;
        }
    }

    public void close() throws HyracksDataException {
        wasCloseCalled = true;
        releaseProcessorFuture.join();
        if (releaseProcessorFuture.isCompletedExceptionally()) {
            try {
                releaseProcessorFuture.get();
                downstreamWriter.fail();
                cleanBufferFrames.forEach(f -> ctx.deallocateFrames(ctx.getInitialFrameSize()));
                ctx.deallocateFrames(downstreamBuffer.getFrameSize());
                ctx.deallocateFrames(activeBufferFrame.getFrameSize());

            } catch (InterruptedException | ExecutionException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private class ReleaseFromBufferProcessor implements Runnable, Serializable {
        private static final long serialVersionUID = 1L;

        private final List<ByteBuffer> localDirtyFrameList;
        private final String threadName;

        private ReleaseFromBufferProcessor(Thread parentThread) {
            this.threadName = parentThread.getName() + "_ReleaseProcessor";
            this.localDirtyFrameList = new ArrayList<>();
        }

        @Override
        public void run() {
            String originalThreadName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(threadName);
                LOGGER.debug("Starting buffer release processor on partition {}.", partition);
                while (true) {
                    Token token = tokenQueue.poll(LOG_FREQUENCY_SECONDS, TimeUnit.SECONDS);
                    if (token == null) {
                        LOGGER.trace("No tokens for the participant frame manager on partition {}.", partition);
                        continue;
                    }

                    // Our base case: we wait for a poison pill from our state manager.
                    if (token == Token.POISON) {
                        LOGGER.debug("Completing buffer release processor on partition {}.", partition);
                        releaseProcessorFuture.complete(null);
                        return;
                    }
                    LOGGER.trace("RELEASE token received on partition {}.", partition);

                    // Part #1: if our buffer is dirty, replace it.
                    captureLock.lock();
                    if (frameTupleAppender.getTupleCount() > 0) {
                        if (cleanBufferFrames.isEmpty()) {
                            // ...if we have no clean frames left, write this to our run file.
                            LOGGER.trace("Active buffer frame is dirty! Writing this to a run file.");
                            if (runFileWriter == null) {
                                String filePrefix = ParticipantFrameManager.class.getSimpleName();
                                FileReference runFile = ctx.getJobletContext().createManagedWorkspaceFile(filePrefix);
                                runFileWriter = new RunFileWriter(runFile, ctx.getIoManager());
                                runFileWriter.open();
                            }
                            runFileWriter.nextFrame(frameTupleAppender.getBuffer());
                            frameTupleAppender.reset(activeBufferFrame, true);

                        } else {
                            // ...otherwise, replace our active buffer frame.
                            LOGGER.trace("Active buffer frame is dirty! Swapping active buffer with clean buffer.");
                            try {
                                dirtyBufferFrames.put(frameTupleAppender.getBuffer());
                                activeBufferFrame.reset(cleanBufferFrames.take());
                                frameTupleAppender.reset(activeBufferFrame, true);

                            } catch (InterruptedException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                    }
                    localDirtyFrameList.clear();
                    dirtyBufferFrames.drainTo(localDirtyFrameList);

                    // Part #2: if we have spilled to disk, build a new reader.
                    if (runFileWriter != null) {
                        runFileWriter.close();
                        runFileReader = runFileWriter.createDeleteOnCloseReader();
                        runFileWriter = null;
                    }
                    captureLock.unlock();

                    // Part #3: release our in-memory buffer...
                    if (!localDirtyFrameList.isEmpty()) {
                        LOGGER.trace("Forwarding frames stored in our memory buffer.");
                        for (ByteBuffer frameTupleBuffer : localDirtyFrameList) {
                            downstreamWriter.nextFrame(frameTupleBuffer);
                            frameTupleBuffer.clear();
                            cleanBufferFrames.put(frameTupleBuffer);
                        }
                    }

                    // Part #4: ...and now release from our run file.
                    if (runFileReader != null) {
                        LOGGER.trace("Forwarding frames stored in our run file.");
                        try {
                            runFileReader.open();
                            while (runFileReader.nextFrame(downstreamBuffer)) {
                                downstreamWriter.nextFrame(downstreamBuffer.getBuffer());
                            }
                        } finally {
                            runFileReader.close();
                        }
                        runFileReader = null;
                    }

                    // Part #5: Finally, forward a marker frame.
                    short sPartition = (short) partition;
                    LOGGER.trace("Pushing marker frame on partition {}.", partition);
                    markerProducer.apply(downstreamBuffer.getBuffer(), sPartition, sPartition, downstreamWriter);
                    LOGGER.trace("Marker frame has been pushed on partition {}.", partition);
                }
            } catch (Exception e) {
                LOGGER.error("Release processor on partition {} raised an error!\n{}", partition, e.getStackTrace());
                releaseProcessorFuture.completeExceptionally(e);

            } finally {
                Thread.currentThread().setName(originalThreadName);
            }
        }
    }
}
