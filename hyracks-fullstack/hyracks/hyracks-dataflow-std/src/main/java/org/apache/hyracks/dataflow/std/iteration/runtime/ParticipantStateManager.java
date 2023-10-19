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

import static org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor.COORDINATOR_PARTITION_INDEX;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.iteration.FixedPointLoggingException;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.hyracks.dataflow.std.iteration.common.State;
import org.apache.hyracks.dataflow.std.iteration.common.Token;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ParticipantStateManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_FREQUENCY_SECONDS = 1;

    // Our participant works in its own thread.
    private CompletableFuture<?> participantFuture;
    private BlockingQueue<Message> messageQueue;
    private BlockingQueue<Token> tokenQueue;
    private CountDownLatch isStartedLatch;
    private final int partition;

    // We manage our "state" here.
    private State state = State.STARTING;

    public ParticipantStateManager(int partition) {
        this.partition = partition;
    }

    public void initialize(FixedPointActivityChannel activityChannel, BlockingQueue<Token> frameManagerQueue,
            BlockingQueue<Message> messageQueue) {
        this.messageQueue = Objects.requireNonNull(messageQueue);
        this.tokenQueue = Objects.requireNonNull(frameManagerQueue);
        this.isStartedLatch = new CountDownLatch(1);
        this.participantFuture = new CompletableFuture<>();
        Executors.newSingleThreadExecutor(Thread::new)
                .submit(new ElectionParticipant(activityChannel, Thread.currentThread()));
    }

    public void close() throws HyracksDataException {
        try {
            isStartedLatch.await();
            transitionTo(State.OBSERVING);
            tokenQueue.put(Token.RELEASE);
            participantFuture.join();
            if (participantFuture.isCompletedExceptionally()) {
                participantFuture.get();
            }
            tokenQueue.put(Token.POISON);

        } catch (InterruptedException | ExecutionException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void fail() throws HyracksDataException {
        try {
            messageQueue.put(Message.POISON_PILL);
            close();

        } catch (InterruptedException e) {
            throw HyracksDataException.create(e);
        }
    }

    private class ElectionParticipant implements Runnable, Serializable {
        private static final long serialVersionUID = 1L;

        // We use the following to communicate with our other tasks.
        private final FixedPointActivityChannel activityChannel;
        private final String threadName;

        private Message message;
        private long timestamp;

        public ElectionParticipant(FixedPointActivityChannel activityChannel, Thread parentThread) {
            this.activityChannel = Objects.requireNonNull(activityChannel);
            this.threadName = parentThread.getName() + "_ElectionParticipant";
        }

        @Override
        public void run() {
            String originalThreadName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(threadName);
                transitionTo(State.WORKING);
                isStartedLatch.countDown();
                while (true) {
                    message = messageQueue.poll(LOG_FREQUENCY_SECONDS, TimeUnit.SECONDS);
                    if (message == null) {
                        LOGGER.trace("No events on participant {}. Currently in state {}.", partition, state);
                        continue;
                    }
                    LOGGER.debug("Event on participant {}: {}.", partition, message);
                    switch (message.getEvent()) {
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
                            participantFuture.complete(null);
                            return;

                        case MARKER:
                            handleMarker();
                            break;

                        default:
                            throw new FixedPointLoggingException(
                                    "Illegal event given to participant " + partition + "!");
                    }
                }

            } catch (Exception e) {
                participantFuture.completeExceptionally(e);

            } finally {
                Thread.currentThread().setName(originalThreadName);
            }
        }

        private void handleMarker() throws HyracksDataException, InterruptedException {
            switch (state) {
                case OBSERVING:
                    if (message.isLive()) {
                        // Our marker has been marked as live somewhere during its traversal. Regenerate.
                        transitionTo(State.OBSERVING);
                        tokenQueue.put(Token.RELEASE);

                    } else {
                        // No operators have raised the live flag. Send REQ to our coordinator.
                        long newTimestamp = timestamp + 1;
                        activityChannel.send(COORDINATOR_PARTITION_INDEX, Event.REQ, newTimestamp);
                        transitionTo(State.WAITING);
                    }
                    break;

                case VOTING_A:
                    if (message.isLive()) {
                        // We transition eagerly back to the OBSERVING state here.
                        transitionTo(State.OBSERVING);
                        activityChannel.send(COORDINATOR_PARTITION_INDEX, Event.NACK_A, timestamp);
                        tokenQueue.put(Token.RELEASE);

                    } else {
                        // Otherwise, inform our coordinator about our vote on A.
                        activityChannel.send(COORDINATOR_PARTITION_INDEX, Event.ACK_A, timestamp);
                    }
                    break;

                case VOTING_B:
                    if (message.isLive()) {
                        // Unlike voting block A, we do not transition eagerly back to OBSERVING.
                        activityChannel.send(COORDINATOR_PARTITION_INDEX, Event.NACK_B, timestamp);

                    } else {
                        activityChannel.send(COORDINATOR_PARTITION_INDEX, Event.ACK_B, timestamp);
                    }
                    break;

                default:
                    throw new FixedPointLoggingException("MARKER event received, but we are not in an expected "
                            + "state! We are in " + state + "!");
            }
        }

        private void handleElectionA() throws HyracksDataException, InterruptedException {
            switch (state) {
                case WAITING:
                    timestamp = message.getTimestamp();
                    transitionTo(State.VOTING_A);
                    tokenQueue.put(Token.RELEASE);
                    break;

                default:
                    throw new FixedPointLoggingException("VOTE_ON_A event received, but we are not in an "
                            + "expected state! We are in " + state + "!");
            }
        }

        private void handleElectionB() throws HyracksDataException, InterruptedException {
            switch (state) {
                case VOTING_A:
                    timestamp = message.getTimestamp();
                    transitionTo(State.VOTING_B);
                    tokenQueue.put(Token.RELEASE);
                    break;

                default:
                    throw new FixedPointLoggingException("VOTE_ON_B event received, but we are not in an "
                            + "expected state! We are in " + state + "!");
            }
        }

        private void handleContinue() throws HyracksDataException, InterruptedException {
            switch (state) {
                case VOTING_A:
                case VOTING_B:
                    // Not all participants are ready to move to VOTING_B or TERMINATE.
                    transitionTo(State.OBSERVING);
                    tokenQueue.put(Token.RELEASE);
                    break;

                case OBSERVING:
                case WAITING:
                    // We have eagerly transitioned to the OBSERVING state.
                    break;

                default:
                    throw new FixedPointLoggingException("CONTINUE event received, but we are not in an "
                            + "expected state! We are in " + state + "!");
            }
        }

        private void handleTerminate() throws HyracksDataException {
            if (state == State.VOTING_B) {
                transitionTo(State.TERMINATING);

            } else {
                throw new FixedPointLoggingException("TERMINATE event received, but we are not in an expected "
                        + " state! We are in " + state + "!");
            }
        }
    }

    private void transitionTo(State destinationState) throws HyracksDataException {
        String stateString = String.format("state %s to state %s", state, destinationState);
        if (!State.isValidTransition(state, destinationState)) {
            throw new FixedPointLoggingException("Illegal transition specified! Can't move from " + stateString + ".");
        }
        LOGGER.debug(String.format("Transitioning from %s on partition %s.", stateString, partition));
        state = destinationState;
    }
}
