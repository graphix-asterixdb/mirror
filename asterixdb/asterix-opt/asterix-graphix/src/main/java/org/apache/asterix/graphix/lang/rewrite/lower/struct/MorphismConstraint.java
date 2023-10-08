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
package org.apache.asterix.graphix.lang.rewrite.lower.struct;

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildSingleAccessorList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * @see org.apache.asterix.graphix.lang.rewrite.lower.action.semantics.MatchSemanticsAction
 */
public class MorphismConstraint {
    private final IsomorphismOperand leftOperand;
    private final IsomorphismOperand rightOperand;

    // We will include any disjunctions within the conjunct itself.
    private final List<Expression> disjunctions = new ArrayList<>();

    public final static class IsomorphismOperand {
        private final List<List<String>> keyFields;
        private final VariableExpr variableExpr;
        private VariableExpr qualifyingVariable;
        private boolean requiresUnknownCheck;

        public IsomorphismOperand(List<List<String>> keyFields, VariableExpr variableExpr) {
            this.keyFields = keyFields;
            this.variableExpr = variableExpr;
            this.requiresUnknownCheck = false;
        }

        public VariableExpr getVariableExpr() {
            return variableExpr;
        }

        @SuppressWarnings("unused")
        public void setQualifyingVariable(VariableExpr qualifyingVariable) {
            this.qualifyingVariable = qualifyingVariable;
        }

        public void setRequiresUnknownCheck(boolean requiresUnknownCheck) {
            this.requiresUnknownCheck = requiresUnknownCheck;
        }
    }

    private MorphismConstraint(IsomorphismOperand leftOperand, IsomorphismOperand rightOperand) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }

    public IsomorphismOperand getLeftOperand() {
        return leftOperand;
    }

    public IsomorphismOperand getRightOperand() {
        return rightOperand;
    }

    public void addDisjunct(Expression disjunctExpr) {
        disjunctions.add(disjunctExpr);
    }

    public Expression build() throws CompilationException {
        Expression leftTerm = leftOperand.variableExpr;
        Expression rightTerm = rightOperand.variableExpr;
        if (leftOperand.qualifyingVariable != null) {
            VarIdentifier leftId = SqlppVariableUtil.toUserDefinedVariableName(leftOperand.variableExpr.getVar());
            leftTerm = new FieldAccessor(leftOperand.qualifyingVariable, leftId);
        }
        if (rightOperand.qualifyingVariable != null) {
            VarIdentifier rightId = SqlppVariableUtil.toUserDefinedVariableName(rightOperand.variableExpr.getVar());
            rightTerm = new FieldAccessor(rightOperand.qualifyingVariable, rightId);
        }

        // Assemble our accessor term.
        List<Expression> conjunctExprs = new ArrayList<>();
        List<FieldAccessor> leftAccessorList = buildSingleAccessorList(leftTerm, leftOperand.keyFields);
        List<FieldAccessor> rightAccessorList = buildSingleAccessorList(rightTerm, rightOperand.keyFields);
        Iterator<FieldAccessor> leftAccessors = new LinkedHashSet<>(leftAccessorList).iterator();
        Iterator<FieldAccessor> rightAccessors = new LinkedHashSet<>(rightAccessorList).iterator();
        while (leftAccessors.hasNext() && rightAccessors.hasNext()) {
            List<Expression> equalityArgs = List.of(leftAccessors.next(), rightAccessors.next());
            OperatorExpr equalityExpr = new OperatorExpr(equalityArgs, List.of(OperatorType.EQ), false);
            List<Expression> argumentList = new ArrayList<>();
            if (leftOperand.requiresUnknownCheck) {
                FunctionSignature isUnknownFunctionId1 = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
                FunctionSignature isNotFunctionId1 = new FunctionSignature(BuiltinFunctions.NOT);
                CallExpr leftUnknownExpr = new CallExpr(isUnknownFunctionId1, List.of(equalityArgs.get(0)));
                CallExpr leftKnownExpr = new CallExpr(isNotFunctionId1, List.of(leftUnknownExpr));
                argumentList.add(leftKnownExpr);
            }
            if (rightOperand.requiresUnknownCheck) {
                FunctionSignature isUnknownFunctionId2 = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
                FunctionSignature isNotFunctionId2 = new FunctionSignature(BuiltinFunctions.NOT);
                CallExpr rightUnknownExpr = new CallExpr(isUnknownFunctionId2, List.of(equalityArgs.get(1)));
                CallExpr rightKnownExpr = new CallExpr(isNotFunctionId2, List.of(rightUnknownExpr));
                argumentList.add(rightKnownExpr);
            }
            argumentList.add(equalityExpr);
            conjunctExprs.add(LowerRewritingUtil.buildConnectedClauses(argumentList, OperatorType.AND));
        }

        // We constrain that all of these fields should never be equal at once.
        Expression accessorTerm = LowerRewritingUtil.buildConnectedClauses(conjunctExprs, OperatorType.AND);
        FunctionSignature isNotFunction = new FunctionSignature(BuiltinFunctions.NOT);
        CallExpr notAccessorExpr = new CallExpr(isNotFunction, List.of(accessorTerm));

        // We will add another layer to handle any disjunctions.
        List<Expression> disjunctExprs = new ArrayList<>(disjunctions);
        disjunctExprs.add(notAccessorExpr);
        return LowerRewritingUtil.buildConnectedClauses(disjunctExprs, OperatorType.OR);
    }

    public static List<MorphismConstraint> generate(List<IsomorphismOperand> operandList) {
        List<MorphismConstraint> morphismConstraints = new ArrayList<>();
        for (int i = 0; i < operandList.size(); i++) {
            for (int j = i + 1; j < operandList.size(); j++) {
                IsomorphismOperand leftOperand = operandList.get(i);
                IsomorphismOperand rightOperand = operandList.get(j);
                morphismConstraints.add(new MorphismConstraint(leftOperand, rightOperand));
            }
        }
        return morphismConstraints;
    }
}
