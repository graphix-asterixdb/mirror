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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.iteration.common.Event;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FixedPointActivityChannel implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    private final IFrameWriter messageWriter;
    private final IFrame messageBufferFrame;
    private final int partition;

    public FixedPointActivityChannel(IFrameWriter messageWriter, IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        this.messageWriter = Objects.requireNonNull(messageWriter);
        this.messageBufferFrame = new VSizeFrame(ctx);
        this.partition = partition;

        // We will also open our writer here.
        this.messageWriter.open();
    }

    public synchronized void send(int destId, Event event, long timestamp) throws HyracksDataException {
        LOGGER.trace("Sending message from partition {}: [{}] ({})-->({})", partition, event, partition,
                (destId == Message.BROADCAST) ? "EVERYONE" : destId);
        ByteBuffer messageBytes = messageBufferFrame.getBuffer();
        Message.setDirectBytes(messageBytes, partition, destId, event, timestamp, false);
        messageWriter.nextFrame(messageBytes);
    }

    public void close() throws HyracksDataException {
        LOGGER.trace("Closing channel on partition {}.", partition);
        messageWriter.close();
    }
}
