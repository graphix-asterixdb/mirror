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
package org.apache.asterix.graphix.algebra.annotations;

import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;

public final class JoinInLoopOperatorAnnotations {
    private static final String JOIN_IN_LOOP_NAME = "JOIN_IN_LOOP";

    public static void markAsJoinInLoop(InnerJoinOperator joinOp) {
        joinOp.getAnnotations().put(JOIN_IN_LOOP_NAME, 1);
    }

    public static boolean isJoinInLoop(InnerJoinOperator joinOp) {
        return joinOp.getAnnotations().containsKey(JOIN_IN_LOOP_NAME);
    }
}
