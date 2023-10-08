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
package org.apache.asterix.graphix.lang.rewrite.lower.action.algorithm;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;

public abstract class AbstractAlgorithmAction implements IEnvironmentAction {
    protected final IEnvironmentAction.Callback loweringCallback;

    // The following are specific to our environment.
    protected GraphixRewritingContext graphixRewritingContext;
    protected AliasLookupTable aliasLookupTable;

    protected AbstractAlgorithmAction(Callback loweringCallback) {
        this.loweringCallback = loweringCallback;
    }

    protected abstract void throwIfNotSupported() throws CompilationException;

    protected abstract void gatherPreliminaries(LoweringEnvironment environment) throws CompilationException;

    protected abstract void decideAndApply(LoweringEnvironment environment) throws CompilationException;

    @Override
    public final void apply(LoweringEnvironment environment) throws CompilationException {
        this.graphixRewritingContext = environment.getGraphixRewritingContext();
        this.aliasLookupTable = environment.getAliasLookupTable();
        throwIfNotSupported();
        gatherPreliminaries(environment);
        decideAndApply(environment);
    }
}
