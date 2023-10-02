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

package org.apache.hyracks.storage.am.common.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;

public class TypeAwareTupleWriterFactory implements ITreeIndexTupleWriterFactory {

    private static final long serialVersionUID = 2L;
    protected final ITypeTraits[] typeTraits;
    protected final ITypeTraits nullTypeTraits;
    protected final INullIntrospector nullIntrospector;

    public TypeAwareTupleWriterFactory(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        this.typeTraits = typeTraits;
        this.nullTypeTraits = nullTypeTraits;
        this.nullIntrospector = nullIntrospector;
    }

    @Override
    public TypeAwareTupleWriter createTupleWriter() {
        return new TypeAwareTupleWriter(typeTraits, nullTypeTraits, nullIntrospector);
    }

}
