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
package org.apache.asterix.graphix.lang.clause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.extension.FromGraphClauseExtension;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class FromGraphClause extends FromClause {
    private final FromGraphClauseExtension clauseExtension;
    private final List<AbstractClause> fromTerms;

    public FromGraphClause(List<? extends AbstractClause> fromTerms) {
        super(Collections.emptyList());
        this.clauseExtension = new FromGraphClauseExtension(this);

        // We will manage our own FROM-TERMs here.
        this.fromTerms = new ArrayList<>();
        this.fromTerms.addAll(fromTerms);
    }

    public List<AbstractClause> getTerms() {
        return fromTerms;
    }

    public List<FromGraphTerm> getFromGraphTerms() {
        return fromTerms.stream().filter(t -> t instanceof FromGraphTerm).map(t -> (FromGraphTerm) t)
                .collect(Collectors.toList());
    }

    @Override
    public List<FromTerm> getFromTerms() {
        return fromTerms.stream().filter(t -> t instanceof FromTerm).map(t -> (FromTerm) t)
                .collect(Collectors.toList());
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        if (visitor instanceof IGraphixLangVisitor) {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);

        } else if (getFromGraphTerms().stream().allMatch(t -> t.getLowerClause() != null)) {
            return visitor.visit(clauseExtension, arg);

        } else {
            return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FromGraphClause that = (FromGraphClause) o;
        return Objects.equals(fromTerms, that.fromTerms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fromTerms);
    }
}
