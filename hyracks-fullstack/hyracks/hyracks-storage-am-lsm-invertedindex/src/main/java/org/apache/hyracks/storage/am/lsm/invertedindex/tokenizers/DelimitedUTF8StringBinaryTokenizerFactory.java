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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DelimitedUTF8StringBinaryTokenizerFactory implements IBinaryTokenizerFactory {

    private static final long serialVersionUID = 1L;
    private final boolean ignoreTokenCount;
    private final boolean sourceHasTypeTag;
    private final ITokenFactory tokenFactory;

    public DelimitedUTF8StringBinaryTokenizerFactory(boolean ignoreTokenCount, boolean sourceHasTypeTag,
            ITokenFactory tokenFactory) {
        this.ignoreTokenCount = ignoreTokenCount;
        this.sourceHasTypeTag = sourceHasTypeTag;
        this.tokenFactory = tokenFactory;
    }

    @Override
    public IBinaryTokenizer createTokenizer() {
        return new DelimitedUTF8StringBinaryTokenizer(ignoreTokenCount, sourceHasTypeTag, tokenFactory);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("tokenFactory", tokenFactory.toJson(registry));
        json.put("ignoreTokenCount", ignoreTokenCount);
        json.put("sourceHasTypeTag", sourceHasTypeTag);
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final ITokenFactory tokenFactory = (ITokenFactory) registry.deserialize(json.get("tokenFactory"));
        final boolean ignoreTokenCount = json.get("ignoreTokenCount").asBoolean();
        final boolean sourceHasTypeTag = json.get("sourceHasTypeTag").asBoolean();
        return new DelimitedUTF8StringBinaryTokenizerFactory(ignoreTokenCount, sourceHasTypeTag, tokenFactory);
    }
}
