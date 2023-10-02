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

import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Push-runtime factory that forwards everything except message frames.
 */
public class MessageSinkRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {
    public MessageSinkRuntimeFactory() {
        super(null);
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new AbstractOneInputOneOutputPushRuntime() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (!FrameHelper.isMessageFrame(buffer)) {
                    writer.nextFrame(buffer);
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
            }
        };
    }
}
