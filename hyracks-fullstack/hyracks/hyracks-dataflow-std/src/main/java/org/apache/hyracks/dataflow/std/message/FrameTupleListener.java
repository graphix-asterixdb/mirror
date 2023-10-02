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
package org.apache.hyracks.dataflow.std.message;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FrameTupleListener extends FrameTupleAccessor {
    private static final Logger LOGGER = LogManager.getLogger();

    private int activeListeners = 0;
    private int numberOfTuples = 0;

    public FrameTupleListener(RecordDescriptor recordDescriptor) {
        super(recordDescriptor);
    }

    public void startListening() {
        activeListeners++;
        LOGGER.trace("New listener has been registered. We now have {} listeners.", activeListeners);
    }

    public void stopListening() {
        activeListeners--;
        if (activeListeners == 0) {
            // Note: we are okay with other processes reporting the hasTuples status of this listen-cycle.
            numberOfTuples = 0;
        }
        LOGGER.trace("Listener has been unregistered. We now have {} listeners.", activeListeners);
    }

    public boolean acceptFrame(ByteBuffer buffer) {
        if (activeListeners == 0) {
            return true;
        }
        reset(buffer);
        int n = getTupleCount();
        if (n == 0) {
            return false;
        }
        LOGGER.trace("Frame has been accepted. Increasing the tuple count by {} from {}.", n, numberOfTuples);
        numberOfTuples += n;
        return true;
    }

    public boolean hasTuples() {
        return numberOfTuples > 0;
    }

    public int getNumberOfTuples() {
        return numberOfTuples;
    }
}
