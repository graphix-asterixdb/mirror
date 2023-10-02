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
package org.apache.asterix.external.dataflow;

import org.apache.asterix.external.util.DataflowUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class TupleForwarder {

    private final FrameTupleAppender appender;
    private final IFrame frame;
    private final IFrameWriter writer;

    public TupleForwarder(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.frame = new VSizeFrame(ctx);
        this.writer = writer;
        this.appender = new FrameTupleAppender(frame);
    }

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        DataflowUtils.addTupleToFrame(appender, tb, writer);
    }

    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    public void complete() throws HyracksDataException {
        appender.write(writer, false);
    }
}
