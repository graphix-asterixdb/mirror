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

import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.DESIGNATED_CONSUMER_START;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.GLOBAL_IDENTIFIER;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.MESSAGE_KIND_START;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.message.MessagePayloadKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractMessageConsumer implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    // We reserve the first six bytes (after the frame size bytes) for message metadata.
    protected final byte consumerDesignation;
    protected final MessagePayloadKind kind;

    protected AbstractMessageConsumer(MessagePayloadKind kind, byte consumerDesignation) {
        this.consumerDesignation = consumerDesignation;
        this.kind = Objects.requireNonNull(kind);
    }

    protected abstract void acceptPayload(ByteBuffer buffer) throws HyracksDataException;

    /**
     * @return True if we have accepted said message frame. Otherwise, false-- we need to handle this frame through
     * other means. Note that this assumes that there is a {@link FrameHelper#isMessageFrame(ByteBuffer)} call on the
     * buffer beforehand (so we assume that we are already working with a marker frame).
     */
    public boolean accept(ByteBuffer buffer) throws HyracksDataException {
        byte[] bytes = buffer.array();

        // Verify that we have the right OP-CODE.
        if (bytes[MESSAGE_KIND_START] != kind.getOpCode()) {
            LOGGER.trace("Message was not intended for this consumer: OP-CODE does not match.");
            return false;
        }

        // Verify that we have the right designation.
        byte consumerByte = bytes[DESIGNATED_CONSUMER_START];
        if (consumerByte != GLOBAL_IDENTIFIER && consumerByte != consumerDesignation) {
            LOGGER.trace("Message was not intended for this consumer: Designation ID does not match.");
            return false;
        }

        // Our payload can be accepted (consumer specific).
        LOGGER.trace("Message has been accepted.");
        acceptPayload(buffer);
        return true;
    }
}
