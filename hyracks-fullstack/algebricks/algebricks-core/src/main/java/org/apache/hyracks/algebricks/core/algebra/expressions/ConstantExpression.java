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

package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public final class ConstantExpression extends AbstractLogicalExpression {
    private final IAlgebricksConstantValue value;

    public static final ConstantExpression TRUE = new ConstantExpression(new IAlgebricksConstantValue() {

        @Override
        public boolean isTrue() {
            return true;
        }

        @Override
        public boolean isMissing() {
            return false;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isFalse() {
            return false;
        }

        @Override
        public String toString() {
            return "true";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IAlgebricksConstantValue) && ((IAlgebricksConstantValue) obj).isTrue();
        }

        @Override
        public int hashCode() {
            return Boolean.TRUE.hashCode();
        }
    });
    public static final ConstantExpression FALSE = new ConstantExpression(new IAlgebricksConstantValue() {

        @Override
        public boolean isTrue() {
            return false;
        }

        @Override
        public boolean isMissing() {
            return false;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isFalse() {
            return true;
        }

        @Override
        public String toString() {
            return "false";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IAlgebricksConstantValue) && ((IAlgebricksConstantValue) obj).isFalse();
        }

        @Override
        public int hashCode() {
            return Boolean.FALSE.hashCode();
        }
    });
    public static final ConstantExpression NULL = new ConstantExpression(new IAlgebricksConstantValue() {

        @Override
        public boolean isTrue() {
            return false;
        }

        @Override
        public boolean isMissing() {
            return false;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean isFalse() {
            return false;
        }

        @Override
        public String toString() {
            return "null";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IAlgebricksConstantValue) && ((IAlgebricksConstantValue) obj).isNull();
        }

        @Override
        public int hashCode() {
            return 0;
        }
    });
    public static final ConstantExpression MISSING = new ConstantExpression(new IAlgebricksConstantValue() {

        @Override
        public boolean isTrue() {
            return false;
        }

        @Override
        public boolean isMissing() {
            return true;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isFalse() {
            return false;
        }

        @Override
        public String toString() {
            return "missing";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IAlgebricksConstantValue) && ((IAlgebricksConstantValue) obj).isMissing();
        }

        @Override
        public int hashCode() {
            return 0;
        }
    });

    public ConstantExpression(IAlgebricksConstantValue value) {
        this.value = value;
    }

    public IAlgebricksConstantValue getValue() {
        return value;
    }

    @Override
    public LogicalExpressionTag getExpressionTag() {
        return LogicalExpressionTag.CONSTANT;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        // do nothing
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConstantExpression)) {
            return false;
        } else {
            return value.equals(((ConstantExpression) obj).getValue());
        }
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitConstantExpression(this, arg);
    }

    @Override
    public AbstractLogicalExpression cloneExpression() {
        ConstantExpression c = new ConstantExpression(value);
        c.setSourceLocation(sourceLoc);
        return c;
    }

    @Override
    public boolean splitIntoConjuncts(List<Mutable<ILogicalExpression>> conjs) {
        return false;
    }
}
