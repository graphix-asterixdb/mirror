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

package org.apache.asterix.lang.sqlpp.clause;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class JoinClause extends AbstractBinaryCorrelateWithConditionClause {

    private final JoinType joinType;

    private Literal.Type outerJoinMissingValueType;

    public JoinClause(JoinType joinType, Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar,
            Expression conditionExpr, Literal.Type outerJoinMissingValueType) {
        super(rightExpr, rightVar, rightPosVar, conditionExpr);
        this.joinType = joinType;
        setOuterJoinMissingValueType(outerJoinMissingValueType);
    }

    public Literal.Type getOuterJoinMissingValueType() {
        return outerJoinMissingValueType;
    }

    public void setOuterJoinMissingValueType(Literal.Type outerJoinMissingValueType) {
        this.outerJoinMissingValueType = validateMissingValueType(joinType, outerJoinMissingValueType);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.JOIN_CLAUSE;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + 31 * joinType.hashCode()
                + (outerJoinMissingValueType != null ? outerJoinMissingValueType.hashCode() : 0);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof JoinClause)) {
            return false;
        }
        JoinClause target = (JoinClause) object;
        return super.equals(target) && joinType.equals(target.getJoinType())
                && Objects.equals(outerJoinMissingValueType, target.outerJoinMissingValueType);
    }

    private static Literal.Type validateMissingValueType(JoinType joinType, Literal.Type missingValueType) {
        switch (joinType) {
            case INNER:
                if (missingValueType != null) {
                    throw new IllegalArgumentException(String.valueOf(missingValueType));
                }
                return null;
            case LEFTOUTER:
            case RIGHTOUTER:
                switch (Objects.requireNonNull(missingValueType)) {
                    case MISSING:
                    case NULL:
                        return missingValueType;
                    default:
                        throw new IllegalArgumentException(String.valueOf(missingValueType));
                }
            default:
                throw new IllegalStateException(String.valueOf(joinType));
        }
    }
}
