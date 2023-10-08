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

import org.apache.asterix.graphix.type.AppendToPathTypeComputer;
import org.apache.asterix.graphix.type.CreateNewPathTypeComputer;
import org.apache.asterix.graphix.type.IsDistinctTypeComputer;
import org.apache.asterix.graphix.type.PathEdgesTypeComputer;
import org.apache.asterix.graphix.type.PathHopCountTypeComputer;
import org.apache.asterix.graphix.type.PathVerticesTypeComputer;
import org.apache.asterix.graphix.type.TranslatePathTypeComputer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphixFunctionSupplier {
    public static final GraphixFunctionSupplier INSTANCE = new GraphixFunctionSupplier();

    public void addToBuiltInFunctions() {
        // Add our private functions...
        FunctionIdentifier createNewOneHopPathID = GraphixFunctionIdentifiers.CREATE_NEW_ONE_HOP_PATH;
        FunctionIdentifier createNewZeroHopPathID = GraphixFunctionIdentifiers.CREATE_NEW_ZERO_HOP_PATH;
        FunctionIdentifier appendToPathID = GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
        FunctionIdentifier translateForwardPathID = GraphixFunctionIdentifiers.TRANSLATE_FORWARD_PATH;
        FunctionIdentifier translateReversePathID = GraphixFunctionIdentifiers.TRANSLATE_REVERSE_PATH;
        FunctionIdentifier countPathHopsID = GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER;
        FunctionIdentifier isDistinctEdgeID = GraphixFunctionIdentifiers.IS_DISTINCT_EDGE;
        FunctionIdentifier isDistinctVertexID = GraphixFunctionIdentifiers.IS_DISTINCT_VERTEX;
        FunctionIdentifier isDistinctEverythingID = GraphixFunctionIdentifiers.IS_DISTINCT_EVERYTHING;
        BuiltinFunctions.addPrivateFunction(createNewOneHopPathID, CreateNewPathTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(createNewZeroHopPathID, CreateNewPathTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(appendToPathID, AppendToPathTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(translateForwardPathID, TranslatePathTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(translateReversePathID, TranslatePathTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(countPathHopsID, PathHopCountTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(isDistinctEdgeID, IsDistinctTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(isDistinctVertexID, IsDistinctTypeComputer.INSTANCE, true);
        BuiltinFunctions.addPrivateFunction(isDistinctEverythingID, IsDistinctTypeComputer.INSTANCE, true);

        // ...and our public functions.
        FunctionIdentifier pathVerticesID = GraphixFunctionIdentifiers.PATH_VERTICES;
        FunctionIdentifier pathEdgesID = GraphixFunctionIdentifiers.PATH_EDGES;
        BuiltinFunctions.addFunction(pathVerticesID, PathVerticesTypeComputer.INSTANCE, true);
        BuiltinFunctions.addFunction(pathEdgesID, PathEdgesTypeComputer.INSTANCE, true);
    }

    private GraphixFunctionSupplier() {
    }
}
