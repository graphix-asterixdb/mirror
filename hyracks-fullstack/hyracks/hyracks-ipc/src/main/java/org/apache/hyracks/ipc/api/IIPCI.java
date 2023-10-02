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
package org.apache.hyracks.ipc.api;

/**
 * The interprocess communication interface that handles communication between different processes across the cluster
 */
public interface IIPCI {

    /**
     * handles the incoming message
     * @param handle
     *            the message IPC handle
     * @param mid
     *            the message id
     * @param rmid
     *            the request message id (if the message is a response to a request)
     * @param payload
     *            the message payload
     */
    void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload);

    /**
     * handles an error message, or failure to unmarshall the message
     * @param handle
     *            the message IPC handle
     * @param mid
     *            the message id
     * @param rmid
     *            the request message id (if the message is a response to a request)
     * @param exception
     *            an exception
     */
    void onError(IIPCHandle handle, long mid, long rmid, Exception exception);
}
