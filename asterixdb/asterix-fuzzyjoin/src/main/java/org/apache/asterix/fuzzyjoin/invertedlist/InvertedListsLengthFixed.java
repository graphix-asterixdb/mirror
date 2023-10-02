/**
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

package org.apache.asterix.fuzzyjoin.invertedlist;

import java.util.Map;

public class InvertedListsLengthFixed implements InvertedLists {
    public final InvertedListLengthFixed[] invertedLists;
    public final Map<Integer, Integer> invertedListsSize;

    public InvertedListsLengthFixed(int noTokens, Map<Integer, Integer> invertedListsSize) {
        invertedLists = new InvertedListLengthFixed[noTokens];
        this.invertedListsSize = invertedListsSize;
    }

    public InvertedList get(int token) {
        return invertedLists[token];
    }

    public void index(int token, int[] element) {
        if (invertedLists[token] == null) {
            // invertedLists[token] = new InvertedListLengthList();
            invertedLists[token] = new InvertedListLengthFixed(invertedListsSize.get(token));
        }
        invertedLists[token].add(element);
    }
}
