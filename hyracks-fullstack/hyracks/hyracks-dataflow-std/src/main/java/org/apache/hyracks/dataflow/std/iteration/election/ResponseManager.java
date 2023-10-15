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
package org.apache.hyracks.dataflow.std.iteration.election;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.iteration.FixedPointException;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResponseManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_FREQUENCY_SECONDS = 1;

    // Our coordinator will run in a separate thread to not interfere with the participant processes.
    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private final CompletableFuture<?> coordinatorFuture;

    @FunctionalInterface
    public interface IMessageSender {
        void send(int destId, Event event, long timestamp) throws HyracksDataException;
    }

    public ResponseManager(int nPartitions, IMessageSender messageSender) {
        coordinatorFuture = CompletableFuture.runAsync(new ElectionCoordinator(messageSender, nPartitions));
    }

    public void accept(Message message) throws HyracksDataException, InterruptedException {
        if (coordinatorFuture.isCompletedExceptionally()) {
            try {
                coordinatorFuture.get();

            } catch (ExecutionException e) {
                throw HyracksDataException.create(e);
            }

        } else if (coordinatorFuture.isDone()) {
            throw new FixedPointException("Coordinator thread has unexpectedly closed!");
        }
        messageQueue.put(message);
    }

    public void close() throws HyracksDataException, InterruptedException {
        accept(Message.POISON_PILL);
        coordinatorFuture.join();
        if (coordinatorFuture.isCompletedExceptionally()) {
            try {
                coordinatorFuture.get();
            } catch (ExecutionException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private class ElectionCoordinator implements Runnable, Serializable {
        private static final long serialVersionUID = 1L;

        private final ElectionAggregate electionAggregate;
        private final List<Integer> bBlockVotingOrder;
        private final IMessageSender messageSender;
        private int bBlockVotingIndex = 0;

        private ElectionCoordinator(IMessageSender messageSender, int nPartitions) {
            this.electionAggregate = new ElectionAggregate(nPartitions);
            this.bBlockVotingOrder = new ArrayList<>(nPartitions);
            this.messageSender = Objects.requireNonNull(messageSender);

            // We will shuffle the array below each time we begin BLOCK_B.
            for (int i = 0; i < nPartitions; i++) {
                this.bBlockVotingOrder.add(i);
            }
            LOGGER.debug("Election coordinator has been initialized.");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Message message = messageQueue.poll(LOG_FREQUENCY_SECONDS, TimeUnit.SECONDS);
                    if (message == null) {
                        LOGGER.trace("No events on coordinator. Current aggregate state is:\n{}", electionAggregate);
                        continue;
                    }
                    LOGGER.debug("Event on coordinator: {}.", message);
                    switch (message.getEvent()) {
                        case REQ:
                            acceptRequest(message);
                            break;

                        case ACK_A:
                        case ACK_B:
                        case NACK_A:
                        case NACK_B:
                            acceptVote(message);
                            break;

                        case POISON:
                            coordinatorFuture.complete(null);
                            return;

                        default:
                            throw new FixedPointException("Illegal event on coordinator!");
                    }

                } catch (Exception e) {
                    coordinatorFuture.completeExceptionally(e);
                }
            }
        }

        private void acceptRequest(Message message) throws HyracksDataException {
            electionAggregate.acceptEvent(message.getSourceId(), message.getEvent(), message.getTimestamp());
            VotingBlock votingBlock = electionAggregate.getVotingBlock(message.getTimestamp());

            // If all participants are WAITING, begin an election for BLOCK_A.
            switch (votingBlock) {
                case WAITING:
                    LOGGER.trace("Initiating election with timestamp {}.", message.getTimestamp());
                    messageSender.send(Message.BROADCAST, Event.VOTE_ON_A, message.getTimestamp());
                    break;

                case WORKING:
                case PERIOD_A:
                    LOGGER.trace("Not all participants are WAITING:\n{}", electionAggregate::toString);
                    break;

                default:
                    throw new FixedPointException("WAITING event received, but we are not in an expected "
                            + "voting block! We are in " + votingBlock + "!");
            }
        }

        private void acceptVote(Message message) throws HyracksDataException {
            electionAggregate.acceptEvent(message.getSourceId(), message.getEvent(), message.getTimestamp());
            VotingBlock votingBlock = electionAggregate.getVotingBlock(message.getTimestamp());
            LOGGER.debug("Vote received for voting BLOCK {}.", votingBlock);

            // Determine if we should respond back to all participants.
            switch (votingBlock) {
                case PERIOD_A:
                    switch (electionAggregate.getElectionResult(message.getTimestamp(), votingBlock)) {
                        case UNANIMOUS:
                            LOGGER.debug("All participants have voted yes on block A.");
                            Collections.shuffle(bBlockVotingOrder);
                            bBlockVotingIndex = 0;
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Sending VOTE_ON_B to participants in random order: [{}]",
                                        bBlockVotingOrder.stream().map(Object::toString)
                                                .collect(Collectors.joining(",")));
                            }
                            int nextPartitionToVote = bBlockVotingOrder.get(bBlockVotingIndex++);
                            LOGGER.trace("Sending BLOCK_B vote request to partition {}.", nextPartitionToVote);
                            messageSender.send(nextPartitionToVote, Event.VOTE_ON_B, message.getTimestamp());
                            return;

                        case BLOCKED:
                            LOGGER.debug("Not all participants have voted yes on block A.");
                            messageSender.send(Message.BROADCAST, Event.CONTINUE, message.getTimestamp());
                            return;

                        case VOTING:
                            LOGGER.trace("Not all participants have voted about block A.\n{}",
                                    electionAggregate::toString);
                            return;
                    }

                case PERIOD_B:
                    switch (electionAggregate.getElectionResult(message.getTimestamp(), votingBlock)) {
                        case UNANIMOUS:
                            LOGGER.debug("All participants have agreed to terminate (yes on B).");
                            messageSender.send(Message.BROADCAST, Event.TERMINATE, message.getTimestamp());
                            return;

                        case BLOCKED:
                            LOGGER.debug("Participant {} has not agreed to terminate (no on B).",
                                    message.getSourceId());
                            messageSender.send(Message.BROADCAST, Event.CONTINUE, message.getTimestamp());
                            return;

                        case VOTING:
                            LOGGER.trace("Not all participants have voted about block B.\n{}",
                                    electionAggregate::toString);
                            int nextPartitionToVote = bBlockVotingOrder.get(bBlockVotingIndex++);
                            LOGGER.trace("Sending BLOCK_B vote request to partition {}.", nextPartitionToVote);
                            messageSender.send(nextPartitionToVote, Event.VOTE_ON_B, message.getTimestamp());
                            return;
                    }
            }
            throw new FixedPointException("ACK/NACK event received, but we are not voting!");
        }
    }
}
