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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.State;

public class ElectionAggregate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final EventChain[] globalEventChain;
    private final Map<Event, Integer> occurrenceMap;
    private final int nParticipants;

    /**
     * Because a participant may eagerly transition back to the {@link State#OBSERVING} state, that participant
     * might send more than one message back to the coordinator. A participant may either send a
     * {@link Event#NACK_A} followed by an {@link Event#REQ}, or a {@link Event#NACK_B} followed by an
     * {@link Event#REQ}.
     * <p>
     * This class keeps track of the past three events submitted (the last one mainly for debugging purposes).
     */
    private static final class EventChain implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long[] timestamps = new long[3];
        private final Event[] eventQueue = new Event[3];

        private EventChain() {
            Arrays.fill(timestamps, 0);
            Arrays.fill(eventQueue, null);
        }

        private void acceptNewEvent(Event event, long timestamp) throws HyracksDataException {
            // Shift our events...
            eventQueue[2] = eventQueue[1];
            eventQueue[1] = eventQueue[0];
            eventQueue[0] = event;

            // ...and their timestamps.
            timestamps[2] = timestamps[1];
            timestamps[1] = timestamps[0];
            timestamps[0] = timestamp;

            // We expect the following patterns of events.
            boolean isValid;
            switch (eventQueue[0]) {
                case REQ:
                    boolean isFirstRequest = eventQueue[1] == null;
                    boolean isSequentialRequest = timestamps[0] > timestamps[1];
                    isValid = isFirstRequest | isSequentialRequest;
                    break;

                case ACK_A:
                case NACK_A:
                    isValid = eventQueue[1] == Event.REQ;
                    break;

                case ACK_B:
                case NACK_B:
                    isValid = eventQueue[1] == Event.ACK_A;
                    break;

                default:
                    throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Illegal event given!");
            }
            if (!isValid) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Illegal sequence of events! " + this);
            }
        }

        private boolean containsEvent(Event event, long timestamp) {
            return eventQueue[0] == event && timestamps[0] == timestamp
                    || eventQueue[1] == event && timestamps[1] == timestamp;
        }

        @Override
        public String toString() {
            final Function<Event, String> eventFormatter = e -> Objects.requireNonNullElse(e, "NULL").toString();
            return String.format("%s[%s] -> %s[%s] -> %s[%s]", eventFormatter.apply(eventQueue[2]), timestamps[2],
                    eventFormatter.apply(eventQueue[1]), timestamps[1], eventFormatter.apply(eventQueue[0]),
                    timestamps[0]);
        }
    }

    public ElectionAggregate(int nParticipants) {
        this.globalEventChain = new EventChain[nParticipants];
        for (int i = 0; i < nParticipants; i++) {
            this.globalEventChain[i] = new EventChain();
        }
        this.nParticipants = nParticipants;
        this.occurrenceMap = new HashMap<>();
        this.occurrenceMap.put(Event.REQ, 0);
        this.occurrenceMap.put(Event.ACK_A, 0);
        this.occurrenceMap.put(Event.ACK_B, 0);
        this.occurrenceMap.put(Event.NACK_A, 0);
        this.occurrenceMap.put(Event.NACK_B, 0);
    }

    public void acceptEvent(int sourceId, Event event, long timestamp) throws HyracksDataException {
        switch (event) {
            case ACK_A:
            case ACK_B:
            case NACK_A:
            case NACK_B:
            case REQ:
                globalEventChain[sourceId].acceptNewEvent(event, timestamp);
                break;
            default:
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Illegal event given!");
        }
    }

    public VotingBlock getVotingBlock(long timestamp) {
        // Iterate through each participant and count the occurrence of each event.
        occurrenceMap.entrySet().forEach(e -> e.setValue(0));
        for (EventChain eventChain : globalEventChain) {
            for (Map.Entry<Event, Integer> mapEntry : occurrenceMap.entrySet()) {
                if (eventChain.containsEvent(mapEntry.getKey(), timestamp)) {
                    mapEntry.setValue(mapEntry.getValue() + 1);
                }
            }
        }

        // Are there **any** votes for period B?...
        if (occurrenceMap.get(Event.ACK_B) + occurrenceMap.get(Event.NACK_B) > 0) {
            return VotingBlock.PERIOD_B;
        }

        // Are there **any** votes for period A?...
        if (occurrenceMap.get(Event.ACK_A) + occurrenceMap.get(Event.NACK_A) > 0) {
            return VotingBlock.PERIOD_A;
        }

        // Is **everyone** waiting?...
        if (occurrenceMap.get(Event.REQ) == nParticipants) {
            return VotingBlock.WAITING;
        }

        // If none of the above hold true, then we have not started an election.
        return VotingBlock.WORKING;
    }

    public ElectionResult getElectionResult(long timestamp, VotingBlock votingBlock) {
        // Iterate through each participant and count the occurrence of each event.
        occurrenceMap.entrySet().forEach(e -> e.setValue(0));
        for (EventChain eventChain : globalEventChain) {
            for (Map.Entry<Event, Integer> mapEntry : occurrenceMap.entrySet()) {
                if (eventChain.containsEvent(mapEntry.getKey(), timestamp)) {
                    mapEntry.setValue(mapEntry.getValue() + 1);
                }
            }
        }

        // Determine the election results.
        switch (votingBlock) {
            case PERIOD_A:
                // We cannot eagerly report on BLOCK_A election results...
                if (occurrenceMap.get(Event.ACK_A) == nParticipants) {
                    return ElectionResult.UNANIMOUS;
                }
                int numberOfAVotes = occurrenceMap.get(Event.ACK_A) + occurrenceMap.get(Event.NACK_A);
                if (numberOfAVotes == nParticipants) {
                    return ElectionResult.BLOCKED;

                } else if (numberOfAVotes > 0 && numberOfAVotes < nParticipants) {
                    return ElectionResult.VOTING;
                }
                throw new IllegalStateException();

            case PERIOD_B:
                // ...but we can report on BLOCK_B results eagerly.
                if (occurrenceMap.get(Event.ACK_B) == nParticipants) {
                    return ElectionResult.UNANIMOUS;

                } else if (occurrenceMap.get(Event.NACK_B) > 0) {
                    return ElectionResult.BLOCKED;

                } else {
                    return ElectionResult.VOTING;
                }

            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < globalEventChain.length; i++) {
            EventChain eventChain = globalEventChain[i];
            sb.append("Participant ").append(i);
            sb.append(": ").append(eventChain);
            if (i < globalEventChain.length - 1) {
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
