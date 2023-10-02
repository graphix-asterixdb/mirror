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
package org.apache.asterix.common.api;

import java.io.Serializable;

public interface IRequestReference extends Serializable {

    /**
     * Gets the system wide unique request id.
     *
     * @return the requests id.
     */
    String getUuid();

    /**
     * Get the node name which received this requests.
     *
     * @return the node name
     */
    String getNode();

    /**
     * Gets the system time at which the request was received.
     *
     * @return the time at which the request was received.
     */
    long getTime();

    /**
     * Gets the user agent from which the request was received.
     *
     * @return user agent from which the request was received.
     */
    String getUserAgent();

    /**
     * Gets the remote address from which the request was received.
     *
     * @return remote address from which the request was received.
     */
    String getRemoteAddr();
}
