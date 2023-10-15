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
package org.apache.hyracks.algebricks.runtime.operators.message;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractDelegateFrameWriter;
import org.apache.hyracks.dataflow.std.message.FrameTupleListener;
import org.apache.hyracks.dataflow.std.message.MessagePayloadKind;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Push-runtime factory decorator that will invoke {@link IFrameWriter#flush()} on a
 * {@link MessagePayloadKind#MARKER_PAYLOAD} receipt.
 */
public class StateReleaseRuntimeFactory extends AbstractPushRuntimeFactory {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    private final AbstractOneInputOneOutputRuntimeFactory delegateFactory;
    private final String delegateClassName;
    private final byte messageDesignation;

    public StateReleaseRuntimeFactory(AbstractOneInputOneOutputRuntimeFactory delegateFactory,
            byte messageDesignation) {
        this.delegateFactory = Objects.requireNonNull(delegateFactory);
        this.delegateClassName = delegateFactory.getClass().getSimpleName();
        this.messageDesignation = messageDesignation;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        final AbstractOneInputOneOutputPushRuntime delegateRuntime = delegateFactory.createOneOutputPushRuntime(ctx);
        final MarkerMessageConsumer markerConsumer = new MarkerMessageConsumer(messageDesignation);
        return new IPushRuntime[] { new IPushRuntime() {
            private FrameTupleListener frameTupleListener;
            private IFrameWriter originalOutputWriter;

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                originalOutputWriter = writer;
                frameTupleListener = new FrameTupleListener(recordDesc);
                IFrameWriter decoratedOutputWriter = new AbstractDelegateFrameWriter(originalOutputWriter) {
                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        if (!frameTupleListener.acceptFrame(buffer)) {
                            return;
                        }
                        super.nextFrame(buffer);
                    }
                };
                delegateRuntime.setOutputFrameWriter(index, decoratedOutputWriter, recordDesc);
            }

            @Override
            public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
                delegateRuntime.setInputRecordDescriptor(index, recordDescriptor);
            }

            @Override
            public void open() throws HyracksDataException {
                delegateRuntime.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (FrameHelper.isMessageFrame(buffer)) {
                    LOGGER.trace("A message has been given to {}.", delegateClassName);
                    if (!markerConsumer.accept(buffer)) {
                        // We have encountered a different message frame not meant for us. Forward this.
                        originalOutputWriter.nextFrame(buffer);
                        return;
                    }

                    // We will flush using our delegate runtime.
                    frameTupleListener.startListening();
                    LOGGER.trace("Flushing our delegate {}.", delegateClassName);
                    delegateRuntime.flush();
                    if (frameTupleListener.hasTuples()) {
                        LOGGER.trace("Our delegate {} has {} tuples. Marker flag has been raised.", delegateClassName,
                                frameTupleListener.getNumberOfTuples());
                        MarkerMessageConsumer.raiseLiveFlag(buffer);
                    }
                    originalOutputWriter.nextFrame(buffer);
                    frameTupleListener.stopListening();

                } else {
                    delegateRuntime.nextFrame(buffer);
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                delegateRuntime.flush();
            }

            @Override
            public void fail() throws HyracksDataException {
                delegateRuntime.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                delegateRuntime.close();
            }
        } };
    }
}
