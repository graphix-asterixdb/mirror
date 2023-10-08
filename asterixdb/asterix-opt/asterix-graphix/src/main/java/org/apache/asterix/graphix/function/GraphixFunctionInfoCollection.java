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

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.graphix.type.PathEdgesTypeComputer;
import org.apache.asterix.graphix.type.PathVerticesTypeComputer;
import org.apache.asterix.graphix.type.TranslatePathTypeComputer;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt32TypeComputer;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class GraphixFunctionInfoCollection {
    private static final Map<FunctionIdentifier, IFunctionInfo> functionInfoMap = new HashMap<>();

    static {
        // The following functions yield boolean values.
        final IResultTypeComputer booleanComputer = ABooleanTypeComputer.INSTANCE_NULLABLE;
        functionInfoMap.put(IS_DISTINCT_VERTEX, new GraphixFunctionInfo(IS_DISTINCT_VERTEX, booleanComputer));
        functionInfoMap.put(IS_DISTINCT_EDGE, new GraphixFunctionInfo(IS_DISTINCT_EDGE, booleanComputer));
        functionInfoMap.put(IS_DISTINCT_EVERYTHING, new GraphixFunctionInfo(IS_DISTINCT_EVERYTHING, booleanComputer));

        // The following functions yield raw path values (we hijack the bitarray type here).
        final IResultTypeComputer pathComputer = new AbstractResultTypeComputer() {
            @Override
            protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) {
                return BuiltinType.ABITARRAY;
            }
        };
        functionInfoMap.put(CREATE_NEW_ONE_HOP_PATH, new GraphixFunctionInfo(CREATE_NEW_ONE_HOP_PATH, pathComputer));
        functionInfoMap.put(CREATE_NEW_ZERO_HOP_PATH, new GraphixFunctionInfo(CREATE_NEW_ZERO_HOP_PATH, pathComputer));
        functionInfoMap.put(APPEND_TO_EXISTING_PATH, new GraphixFunctionInfo(APPEND_TO_EXISTING_PATH, pathComputer));

        // The following function yields integer values.
        final IResultTypeComputer integerComputer = AInt32TypeComputer.INSTANCE_NULLABLE;
        functionInfoMap.put(EDGE_COUNT_FROM_HEADER, new GraphixFunctionInfo(EDGE_COUNT_FROM_HEADER, integerComputer));

        // The following functions yield closed record values.
        final IResultTypeComputer translateComputer = TranslatePathTypeComputer.INSTANCE;
        functionInfoMap.put(TRANSLATE_FORWARD_PATH, new GraphixFunctionInfo(TRANSLATE_FORWARD_PATH, translateComputer));
        functionInfoMap.put(TRANSLATE_REVERSE_PATH, new GraphixFunctionInfo(TRANSLATE_REVERSE_PATH, translateComputer));

        // The following functions yields lists.
        final IResultTypeComputer pathVerticesComputer = PathVerticesTypeComputer.INSTANCE;
        final IResultTypeComputer pathEdgesComputer = PathEdgesTypeComputer.INSTANCE;
        functionInfoMap.put(PATH_VERTICES, new GraphixFunctionInfo(PATH_VERTICES, pathVerticesComputer));
        functionInfoMap.put(PATH_EDGES, new GraphixFunctionInfo(PATH_EDGES, pathEdgesComputer));
    }

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier functionIdentifier) {
        return functionInfoMap.get(functionIdentifier);
    }

    public static class GraphixFunctionInfo extends FunctionInfo {
        private static final long serialVersionUID = 1L;

        public GraphixFunctionInfo(FunctionIdentifier functionIdentifier, IResultTypeComputer typeComputer) {
            // We only have functional functions.
            super(functionIdentifier, typeComputer, true);
        }
    }
}
