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

package org.apache.hyracks.storage.common;

import java.util.Map;

/**
 * Contains necessary parameters that are required to initialize an index accessor.
 */
public interface IIndexAccessParameters {

    /**
     * Gets the modification call back.
     */
    IModificationOperationCallback getModificationCallback();

    /**
     * Gets the search operation call back.
     */
    ISearchOperationCallback getSearchOperationCallback();

    /**
     * Gets additional parameters.
     */
    Map<String, Object> getParameters();

    /**
     * Gets a parameter.
     *
     * @param key   of a parameter
     * @param clazz used to explicitly cast the requested parameter to the required type
     * @param <T>   the required type
     * @return the requested parameter
     */
    <T> T getParameter(String key, Class<T> clazz);
}
