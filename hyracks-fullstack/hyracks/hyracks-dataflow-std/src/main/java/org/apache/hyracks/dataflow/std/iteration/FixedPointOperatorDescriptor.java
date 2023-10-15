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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
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
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.hyracks.dataflow.std.iteration.common.State;
import org.apache.hyracks.dataflow.std.iteration.common.TracerLock;
import org.apache.hyracks.dataflow.std.iteration.election.ResponseManager;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.hyracks.dataflow.std.message.producer.MarkerMessageProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An instance of {@link FixedPointBaseOperatorDescriptor} that handles termination by communicating with other
 * tasks (on other partitions) + sending out marker frames.
 * <p>
 * There are two main factors to the problem of determining termination:
 * <ol>
 *   <li>Marker-frame-leveraging operators (hereafter, MFLOs)
 *   (e.g. {@link org.apache.hyracks.dataflow.std.join.PersistentBuildJoinOperatorDescriptor}) and determining when
 *   to tell MFLOs to release state (e.g. perform spilled-partition JOINing).</li>
 *   <li>Tasks on other partitions, and determining if <b>all partitions</b> have reached a fixed point.</li>
 * </ol>
 * One approach is to stage each fixed-point iteration, but this becomes painfully chatty and slow. The approach given
 * here performs the following, where tasks communicate with a coordinator when they see no local progress:
 * <ol>
 *   <li>Forward all tuples, be it from our ANCHOR input or from our RECURSIVE input. If we have tuples to offer
 *   downstream, we act like a UNION-ALL operator.</li>
 *   <li>When our ANCHOR input calls {@link IFrameWriter#close()}, we push forward an inactive marker frame local to our
 *   partition / site. This will allow us to gauge our progress locally.</li>
 *   <li>If we receive any active markers (i.e. {@link IFrameWriter#nextFrame(ByteBuffer)} from our RECURSIVE input,
 *   then we resend an inactive marker frame.</li>
 *   <li>If we receive an inactive marker frame, then we inform the coordinator about our "perceived" empty-state and
 *   wait.</li>
 *   <li>Once our coordinator asynchronously "sees" that all participants are waiting, our coordinator asks all
 *   participants to confirm. We add this additional step as participants may have received tuples in between our
 *   coordinator receiving the empty-state update from that same participant.</li>
 *   <li>Once our coordinator has received confirmation that all participants are waiting, our coordinator
 *   <i>synchronously</i> asks each participant to reconfirm its empty state.</li>
 *   <li>Once a coordinator sees that all participants have confirmed, then it is guaranteed that we have reached a
 *   fixed point. Only then can we inform all participants to release their resources (for the affected operators) and
 *   call {@link IFrameWriter#close()}.</li>
 * </ol>
 */
public class FixedPointOperatorDescriptor extends FixedPointBaseOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_FREQUENCY_SECONDS = 1;

    // To communicate locally with our tasks, we use in-band communication with marker frames.
    public static final int FIXED_POINT_OUTPUT_INDEX = 0;
    public static final int EVENT_QUEUE_OUTPUT_INDEX = 1;
    public static final int EVENT_QUEUE_INPUT_INDEX = 2;
    private static final int FIXED_POINT_ACTIVITY_ID = 0;
    private static final int EVENT_QUEUE_ACTIVITY_ID = 1;

    // By default, we designate our 0th site to be our coordinator.
    private static final int COORDINATOR_PARTITION_INDEX = 0;

    private final byte consumerDesignation;

    public FixedPointOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, byte consumerDesignation) {
        super(spec, recDesc, 2);
        this.consumerDesignation = consumerDesignation;
        this.outRecDescs[1] = null;

        // We add the necessary self-loop for communication here...
        IConnectorDescriptor loopConnector = new FixedPointConnectorDescriptor(spec);
        spec.connect(loopConnector, this, EVENT_QUEUE_OUTPUT_INDEX, this, EVENT_QUEUE_INPUT_INDEX);
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
     * Our state object, which holds a queue for {@link Message} instances.
     */
    private static class EventQueueStateObject extends AbstractStateObject {
        public BlockingQueue<Message> messageQueue;

        public EventQueueStateObject(JobId jobId, Object id) {
            super(jobId, id);
            messageQueue = new LinkedBlockingQueue<>();
        }
    }

    /**
     * Activity to listen to {@link FixedPointActivityNode} activities on other nodes (in the same operator) about
     * their local state. This is responsible for listening for termination requests, responses, & decisions, and
     * relaying these messages to our local {@link FixedPointActivityNode} (specifically, the anchor task).
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
                private EventQueueStateObject state;

                @Override
                public void open() {
                    Object stateId = new TaskId(new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID), partition);
                    state = new EventQueueStateObject(ctx.getJobletContext().getJobId(), stateId);
                    ctx.setStateObject(state);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // We only care about the first 12 bytes. This is NOT a message frame, nor is it a data frame.
                    try {
                        Message message = Message.fromBytes(buffer);
                        LOGGER.trace("Received message on partition {}: {}.", partition, message);
                        state.messageQueue.put(message);

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

    private class FixedPointActivityNode extends AbstractFixedPointActivityNode {
        private static final long serialVersionUID = 1L;

        public FixedPointActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new FixedPointPushRuntime(ctx, partition, nPartitions);
        }

        private class FixedPointPushRuntime extends AbstractFixedPointPushRuntime {
            private final IFrame downstreamBufferFrame = new VSizeFrame(ctx);
            private final IFrame messageBufferFrame = new VSizeFrame(ctx);
            private final AtomicBoolean isClosed = new AtomicBoolean(true);

            // The EventQueue activity is responsible for creating this queue.
            private BlockingQueue<Message> messageQueue;
            private IFrameWriter messageWriter;
            private Message message;

            public FixedPointPushRuntime(IHyracksTaskContext ctx, int partition, int nPartitions)
                    throws HyracksDataException {
                super(ctx, partition, nPartitions);
            }

            @Override
            protected AnchorInputPushRuntime getAnchorInputRuntime() {
                return new AnchorInputPushRuntime() {
                    private final MarkerMessageProducer markerProducer = new MarkerMessageProducer(consumerDesignation);
                    private final ResponseManager responseManager = (partition == COORDINATOR_PARTITION_INDEX)
                            ? new ResponseManager(nPartitions, this::sendMessage) : null;

                    // The following is used locally by each participant.
                    private long electionTimestamp;

                    @Override
                    public void open() throws HyracksDataException {
                        super.open();
                        messageWriter.open();
                        isClosed.set(false);

                        // We spin-lock until our event-queue is made.
                        ActivityId eventQueueActivityId = new ActivityId(getOperatorId(), EVENT_QUEUE_ACTIVITY_ID);
                        Object stateId = new TaskId(eventQueueActivityId, partition);
                        IStateObject eventState;
                        do {
                            eventState = ctx.getStateObject(stateId);
                        } while (eventState == null);
                        messageQueue = ((EventQueueStateObject) eventState).messageQueue;
                        LOGGER.debug("Event-queue found on partition {}: {}", partition, stateId);
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        if (responseManager != null) {
                            try {
                                responseManager.close();

                            } catch (InterruptedException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                        super.fail();
                    }

                    /**
                     * On {@link IFrameWriter#close()}, we will use this thread to manage our state machine.
                     */
                    @Override
                    public void close() throws HyracksDataException {
                        // Push forward our marker frames.
                        transitionTo(State.OBSERVING);
                        pushMarkerFrame();

                        // Advance our state machine.
                        while (true) {
                            try {
                                message = messageQueue.poll(LOG_FREQUENCY_SECONDS, TimeUnit.SECONDS);
                                if (message == null) {
                                    LOGGER.trace("No events on partition {}. Currently in state {}.", partition,
                                            getCurrentState());
                                    continue;
                                }
                                LOGGER.debug("Event on partition {}: {}.", partition, message);
                                switch (message.getEvent()) {
                                    case REQ:
                                    case ACK_A:
                                    case ACK_B:
                                    case NACK_A:
                                    case NACK_B:
                                        handleCoordinator();
                                        break;

                                    case VOTE_ON_A:
                                        handleElectionA();
                                        break;

                                    case VOTE_ON_B:
                                        handleElectionB();
                                        break;

                                    case CONTINUE:
                                        handleContinue();
                                        break;

                                    case TERMINATE:
                                        // If we are ready, then exit and close downstream.
                                        handleTerminate();
                                        return;

                                    case MARKER:
                                        handleMarker();
                                        break;

                                    default:
                                        throw new FixedPointException("Illegal event on partition " + partition + "!");
                                }
                            } catch (InterruptedException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                    }

                    private void pushMarkerFrame() throws HyracksDataException {
                        LOGGER.trace("Pushing marker frame on partition {}.", partition);
                        ByteBuffer markerBuffer = downstreamBufferFrame.getBuffer();
                        markerProducer.apply(markerBuffer, (short) partition, (short) partition, downstreamWriter);
                        LOGGER.trace("Marker frame has been pushed on partition {}.", partition);
                    }

                    private void handleCoordinator() throws HyracksDataException, InterruptedException {
                        LOGGER.trace("Message to coordinator received: {}", message);
                        if (message.getDestId() != COORDINATOR_PARTITION_INDEX) {
                            // Sanity check: only our coordinator should handle status updates.
                            throw new FixedPointException("Illegal message sent to a participant!");
                        }
                        responseManager.accept(message);
                    }

                    private void handleElectionA() throws HyracksDataException {
                        switch (getCurrentState()) {
                            case WAITING:
                                electionTimestamp = message.getTimestamp();
                                transitionTo(State.VOTING_A);
                                pushMarkerFrame();
                                break;

                            default:
                                throw new FixedPointException("VOTE_ON_A event received, but we are not in an "
                                        + "expected state! We are in " + getCurrentState() + "!");
                        }
                    }

                    private void handleElectionB() throws HyracksDataException {
                        switch (getCurrentState()) {
                            case VOTING_A:
                                electionTimestamp = message.getTimestamp();
                                transitionTo(State.VOTING_B);
                                pushMarkerFrame();
                                break;

                            default:
                                throw new FixedPointException("VOTE_ON_B event received, but we are not in an "
                                        + "expected state! We are in " + getCurrentState() + "!");
                        }
                    }

                    private void handleContinue() throws HyracksDataException {
                        switch (getCurrentState()) {
                            case VOTING_A:
                            case VOTING_B:
                                // Not all participants are ready to move to VOTING_B or TERMINATE.
                                transitionTo(State.OBSERVING);
                                pushMarkerFrame();
                                break;

                            case OBSERVING:
                            case WAITING:
                                // We have eagerly transitioned to the OBSERVING state.
                                break;

                            default:
                                throw new FixedPointException("CONTINUE event received, but we are not in an "
                                        + "expected state! We are in " + getCurrentState() + "!");
                        }
                    }

                    private void handleTerminate() throws HyracksDataException, InterruptedException {
                        if (getCurrentState() == State.VOTING_B) {
                            transitionTo(State.TERMINATING);
                            isClosed.set(true);
                            downstreamWriter.close();
                            messageWriter.close();
                            if (responseManager != null) {
                                responseManager.close();
                            }
                            LOGGER.trace("close() finished on partition {}!", partition);

                        } else {
                            throw new FixedPointException("TERMINATE event received, but we are not in an expected "
                                    + " state! We are in " + getCurrentState() + "!");
                        }
                    }

                    private void handleMarker() throws HyracksDataException, InterruptedException {
                        switch (getCurrentState()) {
                            case OBSERVING:
                                if (message.isLive()) {
                                    // Our marker has been marked as live somewhere during its traversal. Resend.
                                    transitionTo(State.OBSERVING);
                                    pushMarkerFrame();

                                } else {
                                    // No operators have raised the live flag. Send REQ to our coordinator.
                                    long newElectionTimestamp = electionTimestamp + 1;
                                    sendMessage(COORDINATOR_PARTITION_INDEX, Event.REQ, newElectionTimestamp);
                                    transitionTo(State.WAITING);
                                }
                                break;

                            case VOTING_A:
                                if (message.isLive()) {
                                    // We transition eagerly back to the OBSERVING state here.
                                    transitionTo(State.OBSERVING);
                                    sendMessage(COORDINATOR_PARTITION_INDEX, Event.NACK_A, electionTimestamp);
                                    pushMarkerFrame();

                                } else {
                                    // Otherwise, inform our coordinator about our vote on A.
                                    sendMessage(COORDINATOR_PARTITION_INDEX, Event.ACK_A, electionTimestamp);
                                }
                                break;

                            case VOTING_B:
                                if (message.isLive()) {
                                    // Unlike voting block A, we do not transition eagerly back to OBSERVING.
                                    sendMessage(COORDINATOR_PARTITION_INDEX, Event.NACK_B, electionTimestamp);

                                } else {
                                    sendMessage(COORDINATOR_PARTITION_INDEX, Event.ACK_B, electionTimestamp);
                                }
                                break;

                            default:
                                throw new FixedPointException("MARKER event received, but we are not in an expected "
                                        + "state! We are in " + getCurrentState() + "!");
                        }
                    }

                    private void sendMessage(int destId, Event event, long timestamp) throws HyracksDataException {
                        LOGGER.trace("Sending message from partition {}: [{}] ({})-->({})", partition, event, partition,
                                (destId == Message.BROADCAST) ? "EVERYONE" : destId);
                        ByteBuffer messageBytes = messageBufferFrame.getBuffer();
                        Message.setDirectBytes(messageBytes, partition, destId, event, timestamp, false);
                        messageWriter.nextFrame(messageBytes);
                    }
                };
            }

            @Override
            protected RecursiveInputPushRuntime getRecursiveInputRuntime() {
                return new RecursiveInputPushRuntime() {
                    private final MarkerMessageConsumer markerConsumer = new MarkerMessageConsumer(consumerDesignation);
                    private final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(recordDesc);
                    private long receiptTimestamp = 0;

                    @Override
                    public void open() throws HyracksDataException {
                        super.open();
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        if (FrameHelper.isMessageFrame(buffer)) {
                            LOGGER.trace("A marker has been received on partition {}.", partition);
                            if (!markerConsumer.accept(buffer)) {
                                // We drop state-release markers, but forward all other markers.
                                downstreamWriter.nextFrame(buffer);
                                return;
                            }

                            // Inform our anchor input task about our marker receipt.
                            try {
                                LOGGER.trace("Marker received from {} on partition {}.",
                                        markerConsumer.getLastAcceptedProducer(), partition);
                                Message messageToManager = new Message(markerConsumer.getLastAcceptedProducer(),
                                        partition, Event.MARKER, ++receiptTimestamp, markerConsumer.isLiveFlagRaised());
                                messageQueue.put(messageToManager);

                            } catch (InterruptedException e) {
                                throw HyracksDataException.create(e);
                            }
                            return;
                        }

                        // We want to respect the IFrameWriter contract here: do not empty frames (after close).
                        if (isClosed.get()) {
                            frameTupleAccessor.reset(buffer);
                            if (frameTupleAccessor.getTupleCount() == 0) {
                                LOGGER.trace("Frame with no tuples received after close. Dropping this frame.");
                                return;
                            }
                        }

                        // We do not have a marker frame. Handle this frame as normal.
                        super.nextFrame(buffer);
                    }
                };
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                switch (index) {
                    case FIXED_POINT_OUTPUT_INDEX:
                        this.downstreamWriter = new IFrameWriter() {
                            private final ReentrantLock writerLock = TracerLock.get("DownstreamWriter_" + partition);

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
                        this.recordDesc = recordDesc;
                        break;

                    case EVENT_QUEUE_OUTPUT_INDEX:
                        this.messageWriter = (partition != COORDINATOR_PARTITION_INDEX) ? writer : new IFrameWriter() {
                            private final ReentrantLock writerLock = TracerLock.get("MessageWriter_" + partition);

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

                    default:
                        throw new IndexOutOfBoundsException();
                }
            }
        }
    }
}
