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
package org.apache.hyracks.dataflow.std.iteration.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

/**
 * Event given to the anchor / manager thread to handle.
 */
public enum Event {
    // Events from the termination coordinator to all participants.
    VOTE_ON_A,
    VOTE_ON_B,
    CONTINUE,
    TERMINATE,

    // Events from a participant to the termination coordinator.
    REQ,
    ACK_A,
    NACK_A,
    ACK_B,
    NACK_B,
    POISON,

    // Event from the tuple-processing thread to state-machine manager thread.
    MARKER;

    private final static Map<Integer, Event> ordinalEventMap;

    static {
        ordinalEventMap = new HashMap<>();
        for (Event event : Event.values()) {
            ordinalEventMap.put(event.ordinal(), event);
        }
    }

    public static Event fromBytes(byte[] bytes, int start) throws HyracksDataException {
        int v = IntegerPointable.getInteger(bytes, start);
        if (!ordinalEventMap.containsKey(v)) {
            throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Event not recognized! [" + v + "]");
        }
        return ordinalEventMap.get(v);
    }
}
