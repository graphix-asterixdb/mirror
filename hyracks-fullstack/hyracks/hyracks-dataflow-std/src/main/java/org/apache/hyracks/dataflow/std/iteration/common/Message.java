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

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

/**
 * Message class used to communicate between tasks and threads.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * When destId is set to BROADCAST, our connector will forward this message to all tasks.
     */
    public static final int BROADCAST = -1;

    /**
     * When inserted into a queue, all consumers will subsequently shut down.
     */
    public static final Message POISON_PILL = new Message(-1, -1, Event.POISON, -1, false);

    public static final int SOURCE_BYTES_START = Integer.BYTES;
    public static final int DESTINATION_BYTES_START = SOURCE_BYTES_START + Integer.BYTES;
    public static final int EVENT_BYTES_START = DESTINATION_BYTES_START + Integer.BYTES;
    public static final int TIMESTAMP_BYTES_START = EVENT_BYTES_START + Integer.BYTES;
    public static final int LIVE_FLAG_BYTES_START = TIMESTAMP_BYTES_START + Long.BYTES;

    private final int sourceId;
    private final int destId;
    private final Event event;
    private final long timestamp;
    private final boolean liveFlag;

    public Message(int sourceId, int destId, Event event, long timestamp, boolean liveFlag) {
        this.sourceId = sourceId;
        this.destId = destId;
        this.event = event;
        this.timestamp = timestamp;
        this.liveFlag = liveFlag;
    }

    public static void setDirectBytes(ByteBuffer buffer, int sid, int did, Event event, long t, boolean isLive) {
        buffer.clear();
        buffer.putInt(1);
        buffer.putInt(sid);
        buffer.putInt(did);
        buffer.putInt(event.ordinal());
        buffer.putLong(t);
        buffer.put((byte) (isLive ? 1 : 0));
        buffer.position(0);
    }

    public static Message fromBytes(ByteBuffer buffer) throws HyracksDataException {
        byte[] byteArray = buffer.array();
        int sourceId = IntegerPointable.getInteger(byteArray, SOURCE_BYTES_START);
        int destId = IntegerPointable.getInteger(byteArray, DESTINATION_BYTES_START);
        Event event = Event.fromBytes(byteArray, EVENT_BYTES_START);
        long timestamp = LongPointable.getLong(byteArray, TIMESTAMP_BYTES_START);
        boolean liveFlag = byteArray[LIVE_FLAG_BYTES_START] == 1;
        return new Message(sourceId, destId, event, timestamp, liveFlag);
    }

    public int getSourceId() {
        return sourceId;
    }

    public int getDestId() {
        return destId;
    }

    public Event getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isLive() {
        return liveFlag;
    }

    @Override
    public String toString() {
        String liveFlagString = liveFlag ? "LIVE" : "DEAD";
        String eventString = (event == Event.MARKER) ? String.format("%s-%s", event, liveFlagString) : event.toString();
        return String.format("[%s][t = %d] (%s)-->(%s)", eventString, timestamp, sourceId,
                ((destId == BROADCAST) ? "EVERYONE" : destId));
    }
}
