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

import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.BROADCAST_PARTITION;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.COUNTER_PAYLOAD_BYTES_START;
import static org.apache.hyracks.dataflow.std.message.MessageFrameConstants.SOURCE_PARTITION_START;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.std.message.MessagePayloadKind;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CounterMessageConsumer extends AbstractMessageConsumer {
    private static final Logger LOGGER = LogManager.getLogger();

    // We need to keep track of our producer from each partition.
    private final Map<Short, Integer> producerMap = new HashMap<>();
    private short lastAcceptedProducer;

    protected CounterMessageConsumer(MessagePayloadKind kind, byte consumerDesignation) {
        super(kind, consumerDesignation);
    }

    @Override
    protected void acceptPayload(ByteBuffer buffer) throws HyracksDataException {
        int payloadCount = IntSerDeUtils.getInt(buffer.array(), COUNTER_PAYLOAD_BYTES_START);
        lastAcceptedProducer = ShortPointable.getShort(buffer.array(), SOURCE_PARTITION_START);
        if (lastAcceptedProducer != BROADCAST_PARTITION && producerMap.containsKey(lastAcceptedProducer)
                && producerMap.get(lastAcceptedProducer) > payloadCount) {
            int previousPayloadCount = producerMap.get(lastAcceptedProducer);
            throw new HyracksDataException(ErrorCode.ILLEGAL_STATE,
                    String.format("Payload count is not monotonically increasing! Expected %d or %d, but got %d.",
                            previousPayloadCount, previousPayloadCount + 1, payloadCount));
        }
        LOGGER.trace("Payload [{}] from producer partition {} accepted.", payloadCount,
                (lastAcceptedProducer == BROADCAST_PARTITION) ? "BROADCAST" : lastAcceptedProducer);
        producerMap.put(lastAcceptedProducer, payloadCount);
    }

    public int getInvocations(short producerPartition) {
        return producerMap.getOrDefault(producerPartition, 0);
    }

    public short getLastAcceptedProducer() {
        return lastAcceptedProducer;
    }
}
