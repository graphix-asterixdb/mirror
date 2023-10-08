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
package org.apache.asterix.graphix.type;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class TranslatePathTypeComputer extends AbstractResultTypeComputer {
    public static final IResultTypeComputer INSTANCE = new TranslatePathTypeComputer();
    public static final ARecordType DEFAULT_RECORD_TYPE;

    // All materialized paths have two fields.
    public static final String VERTICES_FIELD_NAME = "Vertices";
    public static final String EDGES_FIELD_NAME = "Edges";

    static {
        // TODO (GLENN): We do not have extension types (therefore, no "path" type). Make this better later :-)
        IAType edgeListType = new AOrderedListType(RecordUtil.FULLY_OPEN_RECORD_TYPE, null);
        IAType vertexListType = new AOrderedListType(RecordUtil.FULLY_OPEN_RECORD_TYPE, null);
        DEFAULT_RECORD_TYPE = new ARecordType(null, new String[] { VERTICES_FIELD_NAME, EDGES_FIELD_NAME },
                new IAType[] { edgeListType, vertexListType }, false);
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) {
        IAType argType = strippedInputTypes[0];
        if (argType.getTypeTag() != ATypeTag.BITARRAY) {
            return BuiltinType.ANY;
        }

        // Build a record type here, if our opaque parameters have been set.
        AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) expr;
        if (funcCallExpr.getOpaqueParameters() != null) {
            Object[] opaqueParameters = funcCallExpr.getOpaqueParameters();
            AOrderedListType vertexListType = new AOrderedListType((IAType) opaqueParameters[0], null);
            AOrderedListType edgeListType = new AOrderedListType((IAType) opaqueParameters[1], null);
            return new ARecordType(null, new String[] { VERTICES_FIELD_NAME, EDGES_FIELD_NAME },
                    new IAType[] { vertexListType, edgeListType }, false);
        }
        return DEFAULT_RECORD_TYPE;
    }

    private TranslatePathTypeComputer() {
    }
}
