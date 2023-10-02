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
package org.apache.asterix.om.typecomputer.impl;

import java.util.Arrays;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class RecordAddTypeComputer extends AbstractRecordFunctionTypeComputer {
    public static final RecordAddTypeComputer INSTANCE = new RecordAddTypeComputer();

    private RecordAddTypeComputer() {
    }

    @Override
    public IAType computeTypeImpl(AbstractFunctionCallExpression functionCallExpression, IVariableTypeEnvironment env,
            ARecordType inputRecordType, boolean isOutputMissable, boolean isOutputNullable)
            throws AlgebricksException {
        // If our third argument is missing, then just return the type of our first argument.
        ILogicalExpression arg2 = functionCallExpression.getArguments().get(2).getValue();
        IAType type2 = (IAType) env.getType(arg2);
        IAType actualType2 = TypeComputeUtils.getActualType(type2);
        ATypeTag tag2 = actualType2.getTypeTag();
        if (tag2 == ATypeTag.MISSING) {
            IAType type0 = (IAType) env.getType(functionCallExpression.getArguments().get(0).getValue());
            return wrapTypeWithUnknown(type0, isOutputMissable, isOutputNullable);
        }

        // We expect a constant for our second argument.
        ILogicalExpression arg1 = functionCallExpression.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return wrapTypeWithUnknown(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, isOutputMissable,
                    isOutputNullable);
        }
        ConstantExpression constantExpression = (ConstantExpression) arg1;
        AsterixConstantValue constantValue = (AsterixConstantValue) constantExpression.getValue();
        String newFieldName = ((AString) constantValue.getObject()).getStringValue();

        // If our field is found, return the original record type. Otherwise, add our new field.
        ARecordType outputRecordType = inputRecordType;
        if (!Arrays.asList(inputRecordType.getFieldNames()).contains(newFieldName)) {
            String[] fieldNames = new String[inputRecordType.getFieldNames().length + 1];
            IAType[] fieldTypes = new IAType[inputRecordType.getFieldTypes().length + 1];
            int currentCursorPosition = 0;
            for (; currentCursorPosition < inputRecordType.getFieldNames().length; currentCursorPosition++) {
                fieldNames[currentCursorPosition] = inputRecordType.getFieldNames()[currentCursorPosition];
                fieldTypes[currentCursorPosition] = inputRecordType.getFieldTypes()[currentCursorPosition];
            }
            fieldNames[currentCursorPosition] = newFieldName;
            fieldTypes[currentCursorPosition] = type2;
            String inputTypeName = inputRecordType.getTypeName();
            String outputTypeName = inputTypeName != null ? inputTypeName + "_add_" + newFieldName : null;
            outputRecordType = new ARecordType(outputTypeName, fieldNames, fieldTypes, inputRecordType.isOpen());
        }
        return wrapTypeWithUnknown(outputRecordType, isOutputMissable, isOutputNullable);
    }
}
