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
package org.apache.asterix.common.external;

class NoOpExternalFilterEvaluator implements IExternalFilterEvaluator {
    static final IExternalFilterEvaluator INSTANCE = new NoOpExternalFilterEvaluator();

    private NoOpExternalFilterEvaluator() {
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean isComputedFieldUsed(int index) {
        return false;
    }

    @Override
    public void setValue(int index, String stringValue) {
        throw new IndexOutOfBoundsException("Number of paths is 0");
    }

    @Override
    public boolean evaluate() {
        return true;
    }
}
