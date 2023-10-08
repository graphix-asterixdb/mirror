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
package org.apache.asterix.graphix.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionAliases {
    private static final Map<String, FunctionIdentifier> functionAliasMap;

    static {
        // We don't have many functions... for now. :-)
        functionAliasMap = new HashMap<>();
        functionAliasMap.put("vertices", GraphixFunctionIdentifiers.PATH_VERTICES);
        functionAliasMap.put("edges", GraphixFunctionIdentifiers.PATH_EDGES);
    }

    /**
     * @return {@link FunctionIdentifier} associated with the given function alias. Null otherwise.
     */
    public static FunctionIdentifier getFunctionIdentifier(String aliasName) {
        return functionAliasMap.getOrDefault(aliasName, null);
    }
}
