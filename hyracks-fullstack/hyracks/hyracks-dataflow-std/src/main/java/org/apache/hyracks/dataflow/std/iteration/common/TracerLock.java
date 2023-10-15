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
package org.apache.hyracks.dataflow.std.iteration.common;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// A lock class to make debugging a little easier.
public class TracerLock extends ReentrantLock {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LOG_FREQUENCY_SECONDS = 1;

    private final String loggingName;

    public static ReentrantLock get(String loggingName) {
        if (LOGGER.isTraceEnabled()) {
            return new TracerLock(loggingName);

        } else {
            return new ReentrantLock();
        }
    }

    private TracerLock(String loggingName) {
        this.loggingName = loggingName;
    }

    @Override
    public void lock() {
        Stream<StackTraceElement> stackTraceElementStream = Arrays.stream(Thread.currentThread().getStackTrace());
        LOGGER.trace("Acquiring lock for {} with stack trace:\n{}", loggingName,
                stackTraceElementStream.map(StackTraceElement::toString).collect(Collectors.joining("\n")));
        while (true) {
            try {
                if (tryLock(LOG_FREQUENCY_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.trace("Lock acquired for {}!", loggingName);
                    return;
                }
                LOGGER.trace("Waiting on lock-acquire for {}. Current thread queue:\n{}", loggingName,
                        getQueuedThreads().stream().map(Thread::toString).collect(Collectors.joining("\n")));

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void unlock() {
        LOGGER.trace("Releasing lock for {}.", loggingName);
        super.unlock();
    }
}
