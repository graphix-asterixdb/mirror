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
package org.apache.hyracks.dataflow.std.base;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Base class for {@link IFrameWriter} implementations that decorate an existing {@link IFrameWriter}.
 */
public abstract class AbstractDelegateFrameWriter implements IFrameWriter {
    protected final IFrameWriter delegateWriter;

    public AbstractDelegateFrameWriter(IFrameWriter delegateWriter) {
        this.delegateWriter = delegateWriter;
    }

    @Override
    public void open() throws HyracksDataException {
        delegateWriter.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        delegateWriter.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        delegateWriter.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        delegateWriter.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        delegateWriter.flush();
    }
}
