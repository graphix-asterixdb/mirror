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
package org.apache.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractDelegateFrameWriter;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.message.FrameTupleListener;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StateReleaseOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final AbstractSingleActivityOperatorDescriptor delegateOperatorDescriptor;
    private final String delegateClassName;
    private final byte messageDesignation;

    public StateReleaseOperatorDescriptor(IOperatorDescriptorRegistry spec,
            AbstractSingleActivityOperatorDescriptor delegateOperatorDescriptor, byte messageDesignation) {
        super(spec, 1, 1);
        this.delegateOperatorDescriptor = Objects.requireNonNull(delegateOperatorDescriptor);
        if (delegateOperatorDescriptor.getInputArity() != 1 || delegateOperatorDescriptor.getOutputArity() != 1) {
            throw new IllegalStateException("STATE-RELEASE is only implemented for single-input / output operators!");
        }
        this.delegateClassName = delegateOperatorDescriptor.getClass().getSimpleName();
        this.messageDesignation = messageDesignation;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IRecordDescriptorProvider decoratedRecordDescProvider = new IRecordDescriptorProvider() {
            @Override
            public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                return recordDescProvider.getInputRecordDescriptor(getActivityId(), inputIndex);
            }

            @Override
            public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                return recordDescProvider.getOutputRecordDescriptor(getActivityId(), outputIndex);
            }
        };
        return new StateReleasePushRuntime(ctx, decoratedRecordDescProvider, partition, nPartitions);
    }

    private class StateReleasePushRuntime extends AbstractOperatorNodePushable implements IFrameWriter {
        private final AbstractUnaryInputUnaryOutputOperatorNodePushable delegateNodePushable;
        private final MarkerMessageConsumer markerConsumer = new MarkerMessageConsumer(messageDesignation);
        private final FrameTupleListener frameTupleListener;
        private IFrameWriter originalOutputWriter;

        public StateReleasePushRuntime(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions) throws HyracksDataException {
            this.delegateNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) delegateOperatorDescriptor
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);

            // We want to listen for tuples from our output.
            RecordDescriptor outRecDesc = recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0);
            this.frameTupleListener = new FrameTupleListener(outRecDesc);
        }

        public void initialize() throws HyracksDataException {
            delegateNodePushable.initialize();
        }

        @Override
        public void deinitialize() throws HyracksDataException {
            delegateNodePushable.deinitialize();
        }

        @Override
        public int getInputArity() {
            return 1;
        }

        @Override
        public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
            originalOutputWriter = writer;
            IFrameWriter decoratedOutputWriter = new AbstractDelegateFrameWriter(originalOutputWriter) {
                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (!frameTupleListener.acceptFrame(buffer)) {
                        return;
                    }
                    super.nextFrame(buffer);
                }
            };
            delegateNodePushable.setOutputFrameWriter(index, decoratedOutputWriter, recordDesc);
        }

        @Override
        public final IFrameWriter getInputFrameWriter(int index) {
            return this;
        }

        @Override
        public void open() throws HyracksDataException {
            delegateNodePushable.open();
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
                LOGGER.trace("Flushing our delegate {}.", delegateClassName);
                frameTupleListener.startListening();
                delegateNodePushable.flush();
                if (frameTupleListener.hasTuples()) {
                    LOGGER.trace("Our delegate {} has {} tuples. Marker active flag has been raised.",
                            delegateClassName, frameTupleListener.getNumberOfTuples());
                    MarkerMessageConsumer.raiseLiveFlag(buffer);
                }
                originalOutputWriter.nextFrame(buffer);
                frameTupleListener.stopListening();

            } else {
                delegateNodePushable.nextFrame(buffer);
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            delegateNodePushable.flush();
        }

        @Override
        public void fail() throws HyracksDataException {
            delegateNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            delegateNodePushable.close();
        }
    }

    @Override
    public RecordDescriptor[] getOutputRecordDescriptors() {
        return delegateOperatorDescriptor.getOutputRecordDescriptors();
    }

    @Override
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ICCServiceContext serviceCtx) {
        delegateOperatorDescriptor.contributeSchedulingConstraints(constraintAcceptor, serviceCtx);
    }
}
