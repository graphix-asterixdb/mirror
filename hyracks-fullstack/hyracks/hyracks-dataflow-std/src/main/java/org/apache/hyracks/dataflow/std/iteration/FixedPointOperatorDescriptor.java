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
package org.apache.hyracks.dataflow.std.iteration;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.hyracks.dataflow.std.iteration.common.TLock;
import org.apache.hyracks.dataflow.std.iteration.common.Token;
import org.apache.hyracks.dataflow.std.iteration.runtime.CoordinatorStateManager;
import org.apache.hyracks.dataflow.std.iteration.runtime.FixedPointActivityChannel;
import org.apache.hyracks.dataflow.std.iteration.runtime.ParticipantFrameManager;
import org.apache.hyracks.dataflow.std.iteration.runtime.ParticipantStateManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FixedPointOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    // We expect our first input to be the anchor, and the second to be recursive.
    public static final int ANCHOR_INPUT_INDEX = 0;
    public static final int RECURSIVE_INPUT_INDEX = 1;

    // To communicate locally with our tasks, we use in-band communication with marker frames.
    public static final int FIXED_POINT_OUTPUT_INDEX = 0;
    public static final int EVENT_QUEUE_OUTPUT_INDEX = 1;
    public static final int EVENT_QUEUE_INPUT_INDEX = 2;
    private static final int FIXED_POINT_ACTIVITY_ID = 0;
    private static final int EVENT_QUEUE_ACTIVITY_ID = 1;

    // By default, we designate our 0th site to be our coordinator.
    public static final int COORDINATOR_PARTITION_INDEX = 0;

    private final int bufferSizePages;
    private final byte consumerByte;

    public FixedPointOperatorDescriptor(JobSpecification jobSpecification, RecordDescriptor recordDesc,
            int bufferSizePages, byte consumerByte) {
        super(jobSpecification, 2, 2);
        this.consumerByte = consumerByte;
        this.bufferSizePages = bufferSizePages;
        this.outRecDescs[0] = recordDesc;
        this.outRecDescs[1] = null;

        // We add the necessary self-loop for communication here...
        IConnectorDescriptor loopConnector = new FixedPointConnectorDescriptor(jobSpecification);
        jobSpecification.connect(loopConnector, this, EVENT_QUEUE_OUTPUT_INDEX, this, EVENT_QUEUE_INPUT_INDEX);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        // Add our main fixed-point activity.
        ActivityId fixedPointActivityID = new ActivityId(getOperatorId(), FIXED_POINT_ACTIVITY_ID);
        IActivity fixedPointActivityNode = new FixedPointActivityNode(fixedPointActivityID);
        builder.addActivity(this, fixedPointActivityNode);
        builder.addSourceEdge(ANCHOR_INPUT_INDEX, fixedPointActivityNode, 0);
        builder.addSourceEdge(RECURSIVE_INPUT_INDEX, fixedPointActivityNode, 1);
        builder.addTargetEdge(FIXED_POINT_OUTPUT_INDEX, fixedPointActivityNode, 0);

        // Contribute the activities + edges for inter-task communication. This does NOT add the necessary connector.
        ActivityId eventQueueActivityId = new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID);
        IActivity eventQueueActivityNode = new EventQueueActivityNode(eventQueueActivityId);
        builder.addActivity(this, eventQueueActivityNode);
        builder.addSourceEdge(EVENT_QUEUE_INPUT_INDEX, eventQueueActivityNode, 0);
        builder.addTargetEdge(EVENT_QUEUE_OUTPUT_INDEX, fixedPointActivityNode, 1);
    }

    /**
     * Our state object, which holds three blocking queues for communicating between threads.
     */
    private static class FixedPointStateObject extends AbstractStateObject {
        public final BlockingQueue<Message> toParticipantQueue;
        public final BlockingQueue<Message> toCoordinatorQueue;
        public final BlockingQueue<Token> toFrameManagerQueue;

        public FixedPointStateObject(JobId jobId, Object id) {
            super(jobId, id);
            toCoordinatorQueue = new LinkedBlockingQueue<>();
            toParticipantQueue = new LinkedBlockingQueue<>();
            toFrameManagerQueue = new LinkedBlockingQueue<>();
        }
    }

    /**
     * Activity to listen to {@link FixedPointActivityNode} activities on other nodes (in the same operator) about
     * their local state. This is responsible for listening for termination requests, responses, & decisions, and
     * relaying these messages to our local {@link FixedPointActivityNode}.
     */
    private class EventQueueActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public EventQueueActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private FixedPointStateObject state;

                @Override
                public void open() {
                    Object stateId = new TaskId(new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID), partition);
                    state = new FixedPointStateObject(ctx.getJobletContext().getJobId(), stateId);
                    ctx.setStateObject(state);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // We only care about the first 12 bytes. This is NOT a message frame, nor is it a data frame.
                    try {
                        Message message = Message.fromBytes(buffer);
                        LOGGER.trace("Received message on partition {}: {}.", partition, message);
                        switch (message.getEvent()) {
                            case VOTE_ON_A:
                            case VOTE_ON_B:
                            case CONTINUE:
                            case TERMINATE:
                                state.toParticipantQueue.put(message);
                                break;

                            case REQ:
                            case ACK_A:
                            case NACK_A:
                            case ACK_B:
                            case NACK_B:
                                state.toCoordinatorQueue.put(message);
                                break;

                            default:
                                throw new FixedPointLoggingException(
                                        "Illegal message received on partition " + partition + "!");
                        }

                    } catch (InterruptedException e) {
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void fail() {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private class FixedPointActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public FixedPointActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new FixedPointPushRuntime(ctx, partition, nPartitions);
        }

        private class FixedPointPushRuntime extends AbstractOperatorNodePushable {
            private final IHyracksTaskContext ctx;
            private final int nPartitions;
            private final int partition;

            private IFrameWriter downstreamWriter;
            private IFrameWriter messageWriter;
            private RecordDescriptor recordDesc;

            public FixedPointPushRuntime(IHyracksTaskContext ctx, int partition, int nPartitions) {
                this.ctx = ctx;
                this.partition = partition;
                this.nPartitions = nPartitions;
            }

            @Override
            public void initialize() {
            }

            @Override
            public void deinitialize() {
            }

            @Override
            public int getInputArity() {
                return 2;
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                switch (index) {
                    case ANCHOR_INPUT_INDEX:
                        return new AnchorInputPushRuntime();
                    case RECURSIVE_INPUT_INDEX:
                        return new RecursiveInputPushRuntime();
                    default:
                        throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                switch (index) {
                    case FIXED_POINT_OUTPUT_INDEX:
                        this.recordDesc = recordDesc;
                        this.downstreamWriter = new IFrameWriter() {
                            private final ReentrantLock writerLock = TLock.get("DownstreamWriter_" + partition);

                            @Override
                            public void open() throws HyracksDataException {
                                // We do not expect contention here.
                                writer.open();
                            }

                            @Override
                            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                                writerLock.lock();
                                writer.nextFrame(buffer);
                                writerLock.unlock();
                            }

                            @Override
                            public void fail() throws HyracksDataException {
                                writerLock.lock();
                                writer.fail();
                                writerLock.unlock();
                            }

                            @Override
                            public void close() throws HyracksDataException {
                                writerLock.lock();
                                writer.close();
                                writerLock.unlock();
                            }

                            @Override
                            public void flush() throws HyracksDataException {
                                writerLock.lock();
                                writer.flush();
                                writerLock.unlock();
                            }
                        };
                        break;

                    case EVENT_QUEUE_OUTPUT_INDEX:
                        this.messageWriter = writer;
                        break;

                    default:
                        throw new IndexOutOfBoundsException();
                }
            }

            private class AnchorInputPushRuntime implements IFrameWriter {
                private CoordinatorStateManager coordinatorStateManager = null;
                private ParticipantStateManager participantStateManager = null;
                private FixedPointActivityChannel activityChannel;

                @Override
                public void open() throws HyracksDataException {
                    // Open our message writer.
                    activityChannel = new FixedPointActivityChannel(messageWriter, ctx, partition);

                    // We spin-lock until we have our blocking queues.
                    ActivityId eventQueueActivityId = new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID);
                    Object stateId = new TaskId(eventQueueActivityId, partition);
                    IStateObject eventState;
                    do {
                        eventState = ctx.getStateObject(stateId);
                    } while (eventState == null);
                    FixedPointStateObject fixedPointStateObject = (FixedPointStateObject) eventState;
                    BlockingQueue<Message> toParticipantQueue = fixedPointStateObject.toParticipantQueue;
                    BlockingQueue<Message> toCoordinatorQueue = fixedPointStateObject.toCoordinatorQueue;
                    BlockingQueue<Token> toFrameManagerQueue = fixedPointStateObject.toFrameManagerQueue;
                    LOGGER.debug("State object found for anchor input on partition {}: {}", partition, stateId);

                    // Initialize our state managers here.
                    participantStateManager = new ParticipantStateManager(partition);
                    participantStateManager.initialize(activityChannel, toFrameManagerQueue, toParticipantQueue);
                    if (partition == COORDINATOR_PARTITION_INDEX) {
                        coordinatorStateManager = new CoordinatorStateManager();
                        coordinatorStateManager.initialize(nPartitions, toCoordinatorQueue, activityChannel);
                    }
                    downstreamWriter.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    downstreamWriter.nextFrame(buffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    if (coordinatorStateManager != null) {
                        coordinatorStateManager.close();
                    }
                    participantStateManager.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    participantStateManager.close();
                    if (coordinatorStateManager != null) {
                        coordinatorStateManager.close();
                    }
                    activityChannel.close();
                    downstreamWriter.close();
                    LOGGER.debug("close() finished on partition {}!", partition);
                }
            }

            private class RecursiveInputPushRuntime implements IFrameWriter {
                private ParticipantFrameManager participantFrameManager;

                @Override
                public void open() throws HyracksDataException {
                    // We spin-lock until we have our blocking queues.
                    ActivityId eventQueueActivityId = new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID);
                    Object stateId = new TaskId(eventQueueActivityId, partition);
                    IStateObject eventState;
                    do {
                        eventState = ctx.getStateObject(stateId);
                    } while (eventState == null);
                    FixedPointStateObject fixedPointStateObject = (FixedPointStateObject) eventState;
                    BlockingQueue<Message> toParticipantQueue = fixedPointStateObject.toParticipantQueue;
                    BlockingQueue<Token> toFrameManagerQueue = fixedPointStateObject.toFrameManagerQueue;
                    LOGGER.debug("State object found for recursive input on partition {}: {}", partition, stateId);

                    // Initialize our frame manager here.
                    participantFrameManager = new ParticipantFrameManager();
                    FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(recordDesc);
                    participantFrameManager.initialize(downstreamWriter, consumerByte, partition, toParticipantQueue,
                            toFrameManagerQueue, frameTupleAccessor, bufferSizePages, ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    participantFrameManager.accept(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    participantFrameManager.close();
                }

                @Override
                public void fail() {
                }
            }
        }
    }
}
