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
package org.apache.hyracks.algebricks.core.algebra.properties;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;

public abstract class TypePropagationPolicy {
    public static final TypePropagationPolicy ALL = new TypePropagationPolicy() {

        @Override
        public Object getVarType(LogicalVariable var, IMissableTypeComputer ntc,
                List<LogicalVariable> nonMissableVariableList,
                List<List<LogicalVariable>> correlatedMissableVariableLists,
                List<LogicalVariable> nonNullableVariableList,
                List<List<LogicalVariable>> correlatedNullableVariableLists, ITypeEnvPointer... typeEnvs)
                throws AlgebricksException {
            for (ITypeEnvPointer p : typeEnvs) {
                IVariableTypeEnvironment env = p.getTypeEnv();
                if (env == null) {
                    throw new AlgebricksException(
                            "Null environment for pointer " + p + " in getVarType for var=" + var);
                }
                Object t = env.getVarType(var, nonMissableVariableList, correlatedMissableVariableLists,
                        nonNullableVariableList, correlatedNullableVariableLists);
                if (t != null) {
                    boolean makeNonMissable = !nonMissableVariableList.isEmpty() && ntc.canBeMissing(t)
                            && nonMissableVariableList.contains(var);
                    boolean makeNonNullable = !nonNullableVariableList.isEmpty() && ntc.canBeNull(t)
                            && nonNullableVariableList.contains(var);
                    if (makeNonMissable && makeNonNullable) {
                        return ntc.getNonOptionalType(t);
                    } else if (makeNonMissable) {
                        return ntc.getNonMissableType(t);
                    } else if (makeNonNullable) {
                        return ntc.getNonNullableType(t);
                    } else {
                        return t;
                    }
                }
            }
            return null;
        }
    };

    public abstract Object getVarType(LogicalVariable var, IMissableTypeComputer ntc,
            List<LogicalVariable> nonMissableVariableList, List<List<LogicalVariable>> correlatedMissableVariableLists,
            List<LogicalVariable> nonNullableVariableList, List<List<LogicalVariable>> correlatedNullableVariableLists,
            ITypeEnvPointer... typeEnvs) throws AlgebricksException;
}
