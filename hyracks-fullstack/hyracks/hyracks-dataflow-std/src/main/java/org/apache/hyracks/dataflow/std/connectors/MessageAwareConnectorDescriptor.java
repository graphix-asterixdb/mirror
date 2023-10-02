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
import java.util.BitSet;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.std.base.AbstractConnectorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractDelegateFrameWriter;
import org.apache.hyracks.dataflow.std.message.MessageFrameConstants;

public class MessageAwareConnectorDescriptor extends AbstractConnectorDescriptor {
    protected final static int DEST_PARTITION_START = MessageFrameConstants.DEST_PARTITION_START;

    protected final IConnectorDescriptor delegateConnectorDescriptor;

    public MessageAwareConnectorDescriptor(IConnectorDescriptorRegistry spec, IConnectorDescriptor delegate) {
        super(spec);
        this.delegateConnectorDescriptor = delegate;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        // We will share partition-writers with our delegate. Our delegate will close / fail / flush these.
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; i++) {
            epWriters[i] = edwFactory.createFrameWriter(i);
        }
        IFrameWriter delegatePartitioner = delegateConnectorDescriptor.createPartitioner(ctx, recordDesc,
                receiverIndex -> epWriters[receiverIndex], index, nProducerPartitions, nConsumerPartitions);
        return new AbstractDelegateFrameWriter(delegatePartitioner) {
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (!FrameHelper.isMessageFrame(buffer)) {
                    super.nextFrame(buffer);
                    return;
                }
                epWriters[ShortPointable.getShort(buffer.array(), DEST_PARTITION_START)].nextFrame(buffer);
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int receiverIndex, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        return delegateConnectorDescriptor.createPartitionCollector(ctx, recordDesc, receiverIndex, nProducerPartitions,
                nConsumerPartitions);
    }

    @Override
    public void indicateTargetPartitions(int nProducerPartitions, int nConsumerPartitions, int producerIndex,
            BitSet targetBitmap) {
        delegateConnectorDescriptor.indicateTargetPartitions(nProducerPartitions, nConsumerPartitions, producerIndex,
                targetBitmap);
    }

    @Override
    public void indicateSourcePartitions(int nProducerPartitions, int nConsumerPartitions, int consumerIndex,
            BitSet sourceBitmap) {
        delegateConnectorDescriptor.indicateSourcePartitions(nProducerPartitions, nConsumerPartitions, consumerIndex,
                sourceBitmap);
    }

    @Override
    public boolean allProducersToAllConsumers() {
        return delegateConnectorDescriptor.allProducersToAllConsumers();
    }
}
