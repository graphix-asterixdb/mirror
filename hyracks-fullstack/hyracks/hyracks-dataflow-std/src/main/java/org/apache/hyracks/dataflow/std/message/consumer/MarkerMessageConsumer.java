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
package org.apache.hyracks.dataflow.std.message.consumer;

import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.MARKER_ACTIVE_BYTE;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.MARKER_INACTIVE_BYTE;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.MARKER_PAYLOAD_BYTES_START;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.message.MessagePayloadKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class serves to recognize a marker signal in a message frame.
 */
public class MarkerMessageConsumer extends CounterMessageConsumer {
    private static final Logger LOGGER = LogManager.getLogger();

    // We only care about one bit (technically byte) here.
    private boolean isLiveFlagRaised;

    public MarkerMessageConsumer(byte consumerDesignation) {
        super(MessagePayloadKind.MARKER_PAYLOAD, consumerDesignation);
    }

    public boolean isLiveFlagRaised() {
        return isLiveFlagRaised;
    }

    public static void raiseLiveFlag(ByteBuffer buffer) {
        buffer.array()[MARKER_PAYLOAD_BYTES_START] = MARKER_ACTIVE_BYTE;
        LOGGER.trace("Raising live flag for marker.");
    }

    @Override
    protected void acceptPayload(ByteBuffer buffer) throws HyracksDataException {
        super.acceptPayload(buffer);

        // Immediately after our counter should be our live byte.
        byte liveFlagByte = buffer.array()[MARKER_PAYLOAD_BYTES_START];
        if (liveFlagByte == MARKER_ACTIVE_BYTE) {
            isLiveFlagRaised = true;
            return;

        } else if (liveFlagByte == MARKER_INACTIVE_BYTE) {
            isLiveFlagRaised = false;
            return;
        }
        throw new HyracksDataException(ErrorCode.ILLEGAL_STATE,
                String.format("Unexpected marker payload! Expected 0 or 1, but got %d.", liveFlagByte));
    }
}
