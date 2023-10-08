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
import java.util.function.Consumer;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionIdentifiers {
    private static final Map<String, FunctionIdentifier> functionIdentifierMap;

    // Graphix functions should exist separate from the "ASTERIX_DV" dataverse.
    public static final DataverseName GRAPHIX_DV = DataverseName.createBuiltinDataverseName("graphix");

    /**
     * @return {@link FunctionIdentifier} associated with the given function name. Null otherwise.
     */
    public static FunctionIdentifier getFunctionIdentifier(String functionName) {
        return functionIdentifierMap.getOrDefault(functionName, null);
    }

    // Public functions that can be called on paths.
    public static final FunctionIdentifier PATH_VERTICES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-vertices", 1);
    public static final FunctionIdentifier PATH_EDGES =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "path-edges", 1);

    // Private functions used internally to enforce navigation semantics.
    public static final FunctionIdentifier IS_DISTINCT_EDGE =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "is-distinct-edge", 2);
    public static final FunctionIdentifier IS_DISTINCT_VERTEX =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "is-distinct-vertex", 2);
    public static final FunctionIdentifier IS_DISTINCT_EVERYTHING =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "is-distinct-everything", 3);
    public static final FunctionIdentifier[] IS_DISTINCT_ =
            new FunctionIdentifier[] { IS_DISTINCT_EDGE, IS_DISTINCT_VERTEX, IS_DISTINCT_EVERYTHING };

    // Private functions used internally to manage a path during navigation.
    public static final FunctionIdentifier CREATE_NEW_ONE_HOP_PATH =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "create-new-one-hop-path", 3);
    public static final FunctionIdentifier CREATE_NEW_ZERO_HOP_PATH =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "create-new-zero-hop-path", 1);
    public static final FunctionIdentifier APPEND_TO_EXISTING_PATH =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "append-to-existing-path", 3);
    public static final FunctionIdentifier EDGE_COUNT_FROM_HEADER =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "optimized-edge-count", 1);
    public static final FunctionIdentifier TRANSLATE_FORWARD_PATH =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "translate-forward-path", 1);
    public static final FunctionIdentifier TRANSLATE_REVERSE_PATH =
            new FunctionIdentifier(GRAPHIX_DV.getCanonicalForm(), "translate-reverse-path", 1);

    static {
        // Register the non-internal functions above.
        functionIdentifierMap = new HashMap<>();
        Consumer<FunctionIdentifier> functionRegister = f -> functionIdentifierMap.put(f.getName(), f);
        functionRegister.accept(PATH_VERTICES);
        functionRegister.accept(PATH_EDGES);
    }
}
