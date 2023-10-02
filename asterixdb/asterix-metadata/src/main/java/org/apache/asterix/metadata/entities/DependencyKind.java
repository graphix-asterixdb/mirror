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

package org.apache.asterix.metadata.entities;

import static org.apache.asterix.common.utils.IdentifierUtil.dataset;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.hyracks.algebricks.common.utils.Triple;

public enum DependencyKind {
    DATASET(dependency -> DatasetUtil.getFullyQualifiedDisplayName(dependency.first, dependency.second)),
    FUNCTION(
            dependency -> new FunctionSignature(dependency.first, dependency.second, Integer.parseInt(dependency.third))
                    .toString()),
    TYPE(dependency -> TypeUtil.getFullyQualifiedDisplayName(dependency.first, dependency.second)),
    SYNONYM(dependency -> MetadataUtil.getFullyQualifiedDisplayName(dependency.first, dependency.second));

    private final java.util.function.Function<Triple<DataverseName, String, String>, String> dependencyDisplayNameAccessor;

    DependencyKind(
            java.util.function.Function<Triple<DataverseName, String, String>, String> dependencyDisplayNameAccessor) {
        this.dependencyDisplayNameAccessor = dependencyDisplayNameAccessor;
    }

    public String getDependencyDisplayName(Triple<DataverseName, String, String> dependency) {
        return dependencyDisplayNameAccessor.apply(dependency);
    }

    @Override
    public String toString() {
        return this == DATASET ? dataset() + " (or view)" : name().toLowerCase();
    }
}
