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
package org.apache.asterix.graphix.runtime.pointable;

import org.apache.asterix.graphix.runtime.pointable.consumer.IPointableConsumer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

/**
 * A path consists of a header, a list of vertices, and a list of edges.
 * <ol>
 * <li>A path header consists of 13 bytes, and is formatted as such:
 *  <pre>
 *    [bit-array type tag]      -- 1 byte
 *    [vertex-list end offset]  -- 4 bytes
 *    [edge-list end offset]    -- 4 bytes
 *    [edge-list item count]    -- 4 bytes
 *  </pre></li>
 *  <li>A path's list of vertices starts at 13 bytes and ends at [vertex-list end offset] bytes.</li>
 *  <li>A path's list of edges starts at [vertex-list end offset] bytes and ends at [edge-list end offset] bytes. All
 *  offsets are given from the start of the path itself (not absolute, from the containing byte array).</li>
 * </ol>
 */
public class GraphixPathPointable extends AbstractPointable {
    public static final int HEADER_VERTEX_LIST_END = 1;
    public static final int HEADER_EDGE_LIST_END = 5;
    public static final int HEADER_EDGE_ITEM_COUNT = 9;
    public static final int PATH_HEADER_LENGTH = 13;

    // TODO (GLENN): Create a custom type tag specifically for extensions.
    public static final byte PATH_SERIALIZED_TYPE_TAG = ATypeTag.BITARRAY.serialize();

    // We will set the following on invocation of our "set".
    private final SinglyLinkedListPointable edgeListPointable;
    private final SinglyLinkedListPointable vertexListPointable;

    public GraphixPathPointable(IPointableConsumer vertexListItemCallback, IPointableConsumer edgeListItemCallback) {
        this.vertexListPointable = new SinglyLinkedListPointable(vertexListItemCallback);
        this.edgeListPointable = new SinglyLinkedListPointable(edgeListItemCallback);
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        int absoluteStartOfVertices = start + PATH_HEADER_LENGTH;
        int absoluteEndOfVertices = start + IntegerPointable.getInteger(bytes, start + HEADER_VERTEX_LIST_END);
        int absoluteLengthOfVertices = absoluteEndOfVertices - absoluteStartOfVertices;
        vertexListPointable.set(bytes, absoluteStartOfVertices, absoluteLengthOfVertices);

        int absoluteEndOfEdges = start + IntegerPointable.getInteger(bytes, start + HEADER_EDGE_LIST_END);
        int absoluteLengthOfEdges = absoluteEndOfEdges - absoluteEndOfVertices;
        edgeListPointable.set(bytes, absoluteEndOfVertices, absoluteLengthOfEdges);

        super.set(bytes, start, length);
    }

    public SinglyLinkedListPointable getEdgeListPointable() {
        return edgeListPointable;
    }

    public SinglyLinkedListPointable getVertexListPointable() {
        return vertexListPointable;
    }
}
