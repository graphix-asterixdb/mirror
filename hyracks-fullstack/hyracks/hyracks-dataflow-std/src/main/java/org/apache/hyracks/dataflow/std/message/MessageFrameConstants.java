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

import org.apache.hyracks.api.comm.FrameConstants;

public final class MessageFrameConstants {
    /**
     * Use the following to denote that a consumer does not care about the source / destination.
     */
    public static final byte GLOBAL_IDENTIFIER = 0x00;

    /**
     * Use the following for broadcast messages in the source / destination.
     */
    public static final short BROADCAST_PARTITION = (short) 0xFFFF;

    /**
     * A total of 4 + 2 + 2 + 1 + 1 = 10 bytes come before our payload.
     * <ol>
     *   <li>The first four bytes are reserved for {@link FrameConstants#META_DATA_FRAME_COUNT_OFFSET}.</li>
     *   <li>The next two bytes are reserved for the source partition.</li>
     *   <li>The next two bytes are reserved for the destination partition.</li>
     *   <li>The next byte is reserved for the message frame kind {@link MessagePayloadKind}.</li>
     *   <li>The next byte is reserved for identifying the designated consumer.</li>
     * </ol>
     */
    public static final int SOURCE_PARTITION_START = 4;
    public static final int DEST_PARTITION_START = 6;
    public static final int MESSAGE_KIND_START = 8;
    public static final int DESIGNATED_CONSUMER_START = 9;
    public static final int PAYLOAD_BYTES_START = 10;

    /**
     * The following are payload specific byte starts.
     */
    public static final int COUNTER_PAYLOAD_BYTES_START = PAYLOAD_BYTES_START;
    public static final int MARKER_PAYLOAD_BYTES_START = COUNTER_PAYLOAD_BYTES_START + Integer.BYTES;

    /**
     * The following are special payload values (just marker frames for now).
     */
    public static final byte MARKER_ACTIVE_BYTE = 0x0;
    public static final byte MARKER_INACTIVE_BYTE = 0x1;

    private MessageFrameConstants() {
    }
}
