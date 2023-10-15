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
package org.apache.hyracks.dataflow.std.iteration;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import org.apache.hyracks.dataflow.std.iteration.common.Message;
import org.apache.hyracks.dataflow.std.iteration.common.TracerLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Connector which enables in-band inter-task communication. We do not use message frames here nor data-frames, we only
 * use the first three integer-widths (i.e. 12 bytes) of a frame <b>after</b> the
 * {@link org.apache.hyracks.api.comm.FrameConstants#META_DATA_FRAME_COUNT_OFFSET} integer.
 */
public class FixedPointConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    public FixedPointConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        final Lock[] epWriterLocks = new Lock[nConsumerPartitions];
        final boolean[] isOpen = new boolean[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            epWriters[i] = edwFactory.createFrameWriter(i);
            epWriterLocks[i] = TracerLock.get(FixedPointOperatorDescriptor.class.getName() + "_" + i);
        }
        return new IFrameWriter() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                int destId = IntegerPointable.getInteger(buffer.array(), Message.DESTINATION_BYTES_START);
                if (destId == Message.BROADCAST) {
                    LOGGER.trace("Sending broadcast message on partition " + index + ".");
                    for (int i = 0; i < epWriters.length; i++) {
                        IFrameWriter epWriter = epWriters[i];
                        epWriterLocks[i].lock();
                        buffer.position(0);
                        LOGGER.trace("Sending message to partition " + i + " from partition " + index + ".");
                        epWriter.nextFrame(buffer);
                        epWriterLocks[i].unlock();
                    }
                } else {
                    epWriterLocks[destId].lock();
                    LOGGER.trace("Sending message to partition " + destId + " from partition " + index + ".");
                    buffer.position(0);
                    epWriters[destId].nextFrame(buffer);
                    epWriterLocks[destId].unlock();
                }
            }

            // We copy the failure, close, and open semantics from the M:N broadcast connector.
            @Override
            public void open() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    isOpen[i] = true;
                    epWriters[i].open();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                HyracksDataException failException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].fail();
                        } catch (Throwable th) {
                            if (failException == null) {
                                failException = HyracksDataException.create(th);
                            } else {
                                failException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (failException != null) {
                    throw failException;
                }
            }

            @Override
            public void close() throws HyracksDataException {
                HyracksDataException closeException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].close();
                        } catch (Throwable th) {
                            if (closeException == null) {
                                closeException = HyracksDataException.create(th);
                            } else {
                                closeException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (closeException != null) {
                    throw closeException;
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                for (IFrameWriter writer : epWriters) {
                    writer.flush();
                }
            }
        };
    }
}
