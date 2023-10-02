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
package org.apache.hyracks.dataflow.std.connectors;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.std.base.AbstractDelegateFrameWriter;
import org.apache.hyracks.dataflow.std.message.FrameTupleListener;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StateReleaseConnectorDescriptor extends MessageAwareConnectorDescriptor {
    private final static Logger LOGGER = LogManager.getLogger();
    private final byte markerDesignation;

    public StateReleaseConnectorDescriptor(IConnectorDescriptorRegistry spec,
            IConnectorDescriptor delegateConnectorDescriptor, byte markerDesignation) {
        super(spec, delegateConnectorDescriptor);
        this.markerDesignation = markerDesignation;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        // We will share partition-writers with our delegate. Our delegate will close / fail / flush these.
        final FrameTupleListener[] ftListeners = new FrameTupleListener[nConsumerPartitions];
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; i++) {
            final int indexOfWriter = i;
            ftListeners[i] = new FrameTupleListener(recordDesc);
            epWriters[i] = new AbstractDelegateFrameWriter(edwFactory.createFrameWriter(i)) {
                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (!ftListeners[indexOfWriter].acceptFrame(buffer)) {
                        return;
                    }
                    super.nextFrame(buffer);
                }
            };
        }
        IFrameWriter delegatePartitioner = delegateConnectorDescriptor.createPartitioner(ctx, recordDesc,
                receiverIndex -> epWriters[receiverIndex], index, nProducerPartitions, nConsumerPartitions);
        return new AbstractDelegateFrameWriter(delegatePartitioner) {
            private final MarkerMessageConsumer markerConsumer = new MarkerMessageConsumer(markerDesignation);

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (FrameHelper.isMessageFrame(buffer)) {
                    short destinationPartition = ShortPointable.getShort(buffer.array(), DEST_PARTITION_START);

                    // Check if we have tuples in any of our buffers. If we do, then we'll raise the live flag.
                    if (markerConsumer.accept(buffer)) {
                        for (FrameTupleListener ftListener : ftListeners) {
                            ftListener.startListening();
                        }
                        delegatePartitioner.flush();
                        boolean hasEncounteredTuples = false;
                        for (FrameTupleListener ftListener : ftListeners) {
                            if (ftListener.hasTuples()) {
                                hasEncounteredTuples = true;
                            }
                            ftListener.stopListening();
                        }
                        if (hasEncounteredTuples) {
                            MarkerMessageConsumer.raiseLiveFlag(buffer);
                        }
                    }

                    // Regardless of whether we have a STATE_RELEASE payload or not, we will forward our message.
                    LOGGER.trace("Forwarding marker frame from {} to {}.", destinationPartition, index);
                    epWriters[destinationPartition].nextFrame(buffer);

                } else {
                    super.nextFrame(buffer);
                }
            }
        };
    }
}
