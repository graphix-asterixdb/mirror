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
package org.apache.hyracks.dataflow.std.message.producer;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.message.MessagePayloadKind;

public abstract class AbstractMessageProducer implements Serializable {
    private static final long serialVersionUID = 1L;

    // We reserve some bytes (after the size bytes) for message frame metadata.
    private final byte markerConsumerDesignation;
    private final MessagePayloadKind kind;

    protected AbstractMessageProducer(MessagePayloadKind kind, byte markerConsumerDesignation) {
        this.markerConsumerDesignation = markerConsumerDesignation;
        this.kind = kind;
    }

    protected abstract void applyPayload(ByteBuffer buffer) throws HyracksDataException;

    public void apply(ByteBuffer buffer, short sourcePartition, short destPartition, IFrameWriter writer)
            throws HyracksDataException {
        buffer.clear();

        // Insert our frame size, which should always be 1.
        buffer.position(0);
        buffer.putInt(1);

        // Next, insert our message metadata.
        buffer.putShort(sourcePartition);
        buffer.putShort(destPartition);
        buffer.put(kind.getOpCode());
        buffer.put(markerConsumerDesignation);

        // Next, insert our payload.
        applyPayload(buffer);

        // Finally, denote that this frame is a message.
        buffer.position(buffer.limit() - 4);
        buffer.put(FrameConstants.MESSAGE_FRAME_IDENTIFIER_VALUE);
        buffer.position(0);
        writer.nextFrame(buffer);
    }
}
