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
package org.apache.hyracks.tests.util;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.message.producer.MarkerMessageProducer;

public class MarkerLocalInjectorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final RecordDescriptor recordDescriptor;
    private final byte payloadId;

    public MarkerLocalInjectorOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor,
            byte payloadId) {
        super(spec, 1, 1);
        this.recordDescriptor = recordDescriptor;
        this.outRecDescs[0] = recordDescriptor;
        this.payloadId = payloadId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private MarkerMessageProducer markerProducer;
            private FrameTupleAppender frameTupleAppender;
            private FrameTupleAccessor frameTupleAccessor;
            private IFrame markerFrameBuffer;
            private int tuplesUntilMarker;

            @Override
            public void open() throws HyracksDataException {
                frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
                frameTupleAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                writer.open();
                markerFrameBuffer = new VSizeFrame(ctx);
                markerProducer = new MarkerMessageProducer(payloadId);

                // We will randomize the number of tuples until a marker frame is sent.
                tuplesUntilMarker = 1; // (int) (Math.random() * 100);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameTupleAccessor.reset(buffer);
                int tupleCount = frameTupleAccessor.getTupleCount();
                for (int t = 0; t < tupleCount; t++) {
                    FrameUtils.appendToWriter(writer, frameTupleAppender, frameTupleAccessor, t);
                    if (tuplesUntilMarker-- == 0) {
                        //  After every tuplesUntilMarker, we flush our N-tuple frame...
                        frameTupleAppender.flush(writer);

                        // ...and send a marker frame with a growing integer payload.
                        ByteBuffer markerBuffer = markerFrameBuffer.getBuffer();
                        markerProducer.apply(markerBuffer, (short) partition, (short) partition, writer);
                        tuplesUntilMarker = 1; // (int) (Math.random() * 100);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // We want to push out any tuples we have left.
                frameTupleAppender.write(writer, true);
                writer.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                frameTupleAppender.flush(writer);
            }
        };
    }
}