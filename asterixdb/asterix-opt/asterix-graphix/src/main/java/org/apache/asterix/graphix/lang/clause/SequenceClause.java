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

import java.util.Objects;

import org.apache.asterix.graphix.lang.clause.extension.SequenceClauseExtension;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;

/**
 * A functional equivalent to the {@link FromTerm}, used as a container for lowering directly SQL++ translate-able
 * portions of a {@link FromGraphTerm}. We also maintain a list of {@link LetClause} nodes that bind vertex, edge,
 * and path variables to expressions after the main linked list.
 *
 * @see ClauseSequence
 */
public class SequenceClause extends AbstractExtensionClause {
    private final SequenceClauseExtension sequenceClauseExtension;
    private final ClauseSequence clauseSequence;

    public SequenceClause(ClauseSequence clauseSequence) {
        this.sequenceClauseExtension = new SequenceClauseExtension(this);
        this.clauseSequence = Objects.requireNonNull(clauseSequence);
    }

    public ClauseSequence getClauseSequence() {
        return clauseSequence;
    }

    @Override
    public IVisitorExtension getVisitorExtension() {
        return sequenceClauseExtension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clauseSequence.hashCode());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SequenceClause)) {
            return false;
        }
        SequenceClause that = (SequenceClause) object;
        return Objects.equals(this.clauseSequence, that.clauseSequence);
    }

    @Override
    public String toString() {
        return clauseSequence.toString();
    }
}
