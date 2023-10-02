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
package org.apache.asterix.api.http.server;

public class ServletConstants {
    public static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";
    public static final String RESULTSET_ATTR = "org.apache.asterix.RESULTSET";
    public static final String ASTERIX_APP_CONTEXT_INFO_ATTR = "org.apache.asterix.APP_CONTEXT_INFO";
    public static final String EXECUTOR_SERVICE_ATTR = "org.apache.asterix.EXECUTOR_SERVICE";
    public static final String RUNNING_QUERIES_ATTR = "org.apache.asterix.RUNINNG_QUERIES";
    public static final String SERVICE_CONTEXT_ATTR = "org.apache.asterix.SERVICE_CONTEXT";
    public static final String CREDENTIAL_MAP = "org.apache.asterix.CREDENTIAL_MAP";
    public static final String SYS_AUTH_HEADER = "org.apache.asterix.SYS_AUTH_HEADER";

    private ServletConstants() {
    }
}
