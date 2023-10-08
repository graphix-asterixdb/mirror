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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * The logical starting AST node for Graphix queries. Lowering a Graphix AST involves setting the
 * {@link AbstractExtensionClause}, initially set to null. A {@link FromGraphTerm} node includes the following:
 * <ul>
 *  <li>A [{@link DataverseName}, {@link Identifier}] pair.</li>
 *  <li>A list of {@link MatchClause} nodes, with a minimum size of one. The first MATCH node type must always be
 *  {@link org.apache.asterix.graphix.lang.optype.MatchType#LEADING}.</li>
 *  <li>A list of {@link AbstractBinaryCorrelateClause} nodes, which may be empty. These include
 *  {@link org.apache.asterix.lang.sqlpp.clause.UnnestClause} and
 *  {@link org.apache.asterix.lang.sqlpp.clause.JoinClause}.</li>
 * </ul>
 */
public class FromGraphTerm extends AbstractClause {
    private final DataverseName dataverseName;
    private final Identifier graphName;

    // Every non-lowered FROM-GRAPH-TERM **MUST** include at-least a single MATCH clause.
    private final List<MatchClause> matchClauses;
    private final List<AbstractBinaryCorrelateClause> correlateClauses;

    // After lowering, we should have built an extension clause of some sort.
    private AbstractExtensionClause lowerClause = null;

    public FromGraphTerm(DataverseName dataverseName, Identifier graphName, List<MatchClause> matchClauses,
            List<AbstractBinaryCorrelateClause> correlateClauses) {
        this.dataverseName = dataverseName;
        this.graphName = Objects.requireNonNull(graphName);
        this.matchClauses = Objects.requireNonNull(matchClauses);
        this.correlateClauses = Objects.requireNonNull(correlateClauses);
        if (matchClauses.isEmpty()) {
            throw new IllegalArgumentException("FROM-GRAPH-TERM requires at least one MATCH clause.");
        }
    }

    public FromGraphTerm(AbstractExtensionClause lowerClause) {
        this.lowerClause = Objects.requireNonNull(lowerClause);
        this.matchClauses = Collections.emptyList();
        this.correlateClauses = Collections.emptyList();
        this.dataverseName = null;
        this.graphName = null;
    }

    public FromGraphTerm(AbstractExtensionClause lowerClause, List<MatchClause> matchClauses) {
        this.lowerClause = Objects.requireNonNull(lowerClause);
        this.matchClauses = Objects.requireNonNull(matchClauses);
        this.correlateClauses = Collections.emptyList();
        this.dataverseName = null;
        this.graphName = null;
    }

    public GraphIdentifier createGraphIdentifier(MetadataProvider metadataProvider) {
        DataverseName defaultDataverse = metadataProvider.getDefaultDataverseName();
        DataverseName dataverseName = Objects.requireNonNullElse(this.dataverseName, defaultDataverse);
        return new GraphIdentifier(dataverseName, graphName.getValue());
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public Identifier getGraphName() {
        return graphName;
    }

    public List<MatchClause> getMatchClauses() {
        return matchClauses;
    }

    public List<AbstractBinaryCorrelateClause> getCorrelateClauses() {
        return correlateClauses;
    }

    public AbstractExtensionClause getLowerClause() {
        return lowerClause;
    }

    public void setLowerClause(AbstractExtensionClause lowerClause) {
        this.lowerClause = lowerClause;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String toString() {
        if (lowerClause != null) {
            return lowerClause.toString();

        } else if (dataverseName != null && graphName != null) {
            return dataverseName + "." + graphName.getValue();

        } else if (dataverseName == null && graphName != null) {
            return graphName.getValue();
        }
        throw new IllegalStateException();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, graphName, matchClauses, correlateClauses, lowerClause);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FromGraphTerm)) {
            return false;
        }
        FromGraphTerm that = (FromGraphTerm) object;
        return Objects.equals(dataverseName, that.dataverseName) && Objects.equals(graphName, that.graphName)
                && matchClauses.equals(that.matchClauses) && Objects.equals(correlateClauses, that.correlateClauses)
                && Objects.equals(lowerClause, that.lowerClause);
    }

    @Override
    public ClauseType getClauseType() {
        return null;
    }
}
