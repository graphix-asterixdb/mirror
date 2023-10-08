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
package org.apache.asterix.graphix.lang.rewrite;

import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;

public class GraphixRewriterFactory extends SqlppRewriterFactory {
    private final GraphixParserFactory parserFactory;

    public GraphixRewriterFactory(IParserFactory parserFactory) {
        super(parserFactory);

        // We safely downcast the parser factory here.
        this.parserFactory = (GraphixParserFactory) parserFactory;
    }

    @Override
    public IQueryRewriter createQueryRewriter() {
        return new GraphixQueryRewriter(parserFactory);
    }
}
