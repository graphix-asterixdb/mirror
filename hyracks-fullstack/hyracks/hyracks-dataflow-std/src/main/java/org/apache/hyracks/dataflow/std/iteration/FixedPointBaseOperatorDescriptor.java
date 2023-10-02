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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.iteration.common.State;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The base implementation of a fixed-point operator, that repeatedly pushes tuples until no tuples are received.
 * <ol>
 *   <li>We designate two inputs ANCHOR and RECURSIVE. After {@link IFrameWriter#open()}, we forward all tuples from
 *   both our ANCHOR and RECURSIVE downstream.</li>
 *   <li>We expect our ANCHOR to call {@link IFrameWriter#close()} before receiving a {@link IFrameWriter#close()}
 *   call from our RECURSIVE.</li>
 * </ol>
 * Note that this operator does not consider stopping (i.e. how do we know that our RECURSIVE input is finished?),
 * and will never actually terminate. See {@link FixedPointOperatorDescriptor} for how termination is
 * handled in a distributed setting.
 */
public abstract class FixedPointBaseOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    // We expect our first input to be the anchor, and the second to be recursive.
    public static final int ANCHOR_INPUT_INDEX = 0;
    public static final int RECURSIVE_INPUT_INDEX = 1;

    public FixedPointBaseOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDesc,
            int outputArity) {
        super(spec, 2, outputArity);
        outRecDescs[0] = recordDesc;
    }

    protected abstract static class AbstractFixedPointActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public AbstractFixedPointActivityNode(ActivityId id) {
            super(id);
        }

        protected abstract static class AbstractFixedPointPushRuntime extends AbstractOperatorNodePushable {
            protected final IHyracksTaskContext ctx;
            protected RecursiveInputPushRuntime recursiveInputRuntime;
            protected AnchorInputPushRuntime anchorInputRuntime;
            protected RecordDescriptor recordDesc;
            protected final int partition;
            protected final int nPartitions;
            protected IFrameWriter downstreamWriter;

            // We will manage state transitions here.
            private State state;

            public AbstractFixedPointPushRuntime(IHyracksTaskContext ctx, int partition, int nPartitions) {
                this.state = State.STARTING;
                this.ctx = ctx;
                this.partition = partition;
                this.nPartitions = nPartitions;
            }

            @Override
            public final int getInputArity() {
                return 2;
            }

            @Override
            public final IFrameWriter getInputFrameWriter(int index) {
                switch (index) {
                    case ANCHOR_INPUT_INDEX:
                        anchorInputRuntime = getAnchorInputRuntime();
                        return anchorInputRuntime;
                    case RECURSIVE_INPUT_INDEX:
                        recursiveInputRuntime = getRecursiveInputRuntime();
                        return recursiveInputRuntime;
                    default:
                        throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public void initialize() throws HyracksDataException {
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            protected void transitionTo(State destinationState) throws HyracksDataException {
                String stateString = String.format("state %s to state %s", state, destinationState);
                if (!State.isValidTransition(state, destinationState)) {
                    throw new FixedPointException("Illegal transition specified! Can't move from " + stateString + ".");
                }
                LOGGER.debug(String.format("Transitioning from %s on partition %s.", stateString, partition));
                state = destinationState;
            }

            protected State getCurrentState() {
                return state;
            }

            protected abstract AnchorInputPushRuntime getAnchorInputRuntime();

            protected abstract RecursiveInputPushRuntime getRecursiveInputRuntime();

            protected class AnchorInputPushRuntime implements IFrameWriter {
                @Override
                public void open() throws HyracksDataException {
                    downstreamWriter.open();
                    transitionTo(State.WORKING);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    downstreamWriter.nextFrame(buffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    downstreamWriter.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "close() not implemented!");
                }

                @Override
                public void flush() throws HyracksDataException {
                    downstreamWriter.flush();
                }
            }

            protected class RecursiveInputPushRuntime implements IFrameWriter {
                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    downstreamWriter.nextFrame(buffer);
                }

                // We do not touch our downstream operators here, these should be handled by our anchor.
                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }
            }
        }
    }

    // Note: we use this error to help development: if we encounter deadlocks, errors do not bubble back up.
    protected static class FixedPointException extends HyracksDataException {
        public FixedPointException(String errorMessage) {
            super(ErrorCode.ILLEGAL_STATE, errorMessage);
            LOGGER.fatal(errorMessage);
        }
    }
}