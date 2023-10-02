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
package org.apache.hyracks.http.api;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.server.HttpServer;

import io.netty.handler.codec.http.HttpMethod;

/**
 * Represents a component that handles IServlet requests
 */
public interface IServlet {

    /**
     * @return an array of paths associated with this IServlet
     */
    String[] getPaths();

    /**
     * @return the context associated with this IServlet
     */
    ConcurrentMap<String, Object> ctx();

    /**
     * handle the IServletRequest writing the response in the passed IServletResponse
     *
     * @param request
     * @param response
     */
    void handle(IServletRequest request, IServletResponse response);

    /**
     * Get the handler for channel close events
     *
     * @param server
     *            the http server
     * @return the handler for channel close events
     */
    default IChannelClosedHandler getChannelClosedHandler(HttpServer server) {
        return server.getChannelClosedHandler();
    }

    /**
     * Called at server startup to initialize the servlet
     */
    default void init() throws IOException {
    }

    /**
     * @return {@code true} if the servlet ignores query parameters.
     */
    boolean ignoresQueryParameters(HttpMethod method);
}
