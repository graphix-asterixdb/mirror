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
package org.apache.hyracks.algebricks.core.algebra.base;

public interface OperatorAnnotations {
    // hints
    public static final String USE_HASH_GROUP_BY = "USE_HASH_GROUP_BY"; // -->
    public static final String USE_EXTERNAL_GROUP_BY = "USE_EXTERNAL_GROUP_BY"; // -->
    public static final String USE_STATIC_RANGE = "USE_STATIC_RANGE"; // -->
    public static final String USE_DYNAMIC_RANGE = "USE_DYNAMIC_RANGE";
    // Boolean
    public static final String CARDINALITY = "CARDINALITY"; // -->
    // Integer
    public static final String MAX_NUMBER_FRAMES = "MAX_NUMBER_FRAMES"; // -->
    // Integer
    public static final String OP_INPUT_CARDINALITY = "INPUT_CARDINALITY";
    public static final String OP_OUTPUT_CARDINALITY = "OUTPUT_CARDINALITY";
    public static final String OP_COST_TOTAL = "TOTAL_COST";
    public static final String OP_COST_LOCAL = "OP_COST";
    public static final String OP_LEFT_EXCHANGE_COST = "LEFT_EXCHANGE_COST";
    public static final String OP_RIGHT_EXCHANGE_COST = "RIGHT_EXCHANGE_COST";
}
