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
package org.apache.hyracks.dataflow.std.iteration.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * State of the fixed-point state machine. Local to a single thread (the anchor / manager thread).
 */
public enum State {
    STARTING,
    WORKING,
    OBSERVING,
    WAITING,
    VOTING_A,
    VOTING_B,
    TERMINATING;

    private static final Map<State, List<State>> transitionMap;

    static {
        transitionMap = new HashMap<>();
        transitionMap.put(null, List.of(STARTING));
        transitionMap.put(STARTING, List.of(WORKING));
        transitionMap.put(WORKING, List.of(OBSERVING));
        transitionMap.put(OBSERVING, List.of(OBSERVING, WAITING));
        transitionMap.put(WAITING, List.of(WAITING, VOTING_A));
        transitionMap.put(VOTING_A, List.of(OBSERVING, VOTING_B));
        transitionMap.put(VOTING_B, List.of(OBSERVING, TERMINATING));
        transitionMap.put(TERMINATING, Collections.emptyList());
    }

    public static boolean isValidTransition(State fromState, State toState) {
        return transitionMap.get(fromState).contains(toState);
    }
}