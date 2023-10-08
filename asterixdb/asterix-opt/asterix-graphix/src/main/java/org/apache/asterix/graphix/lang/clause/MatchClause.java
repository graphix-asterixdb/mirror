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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrite.lower.action.semantics.MatchSemanticsAction;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * Container for a collection of {@link QueryPatternExpr} AST nodes.
 * <ul>
 *  <li>A MATCH node has three types: LEADING (indicating that this node is first), INNER (indicating that this node
 *  is not first, but all patterns must be matched), and LEFTOUTER (indicating that this node is optionally
 *  matched).</li>
 *  <li>Under isomorphism semantics, two patterns in different MATCH nodes (one pattern in a LEADING MATCH node and
 *  one pattern in an INNER MATCH node) are equivalent to two patterns in a single LEADING MATCH node. See
 *  {@link MatchSemanticsAction} for more detail.</li>
 * </ul>
 */
public class MatchClause extends AbstractClause implements Iterable<QueryPatternExpr> {
    private final List<QueryPatternExpr> patternExpressions;
    private final MatchType matchType;

    public MatchClause(List<QueryPatternExpr> patternExpressions, MatchType matchType) {
        this.patternExpressions = Objects.requireNonNull(patternExpressions);
        this.matchType = Objects.requireNonNull(matchType);
    }

    public MatchType getMatchType() {
        return matchType;
    }

    @Override
    public Iterator<QueryPatternExpr> iterator() {
        return patternExpressions.iterator();
    }

    @Override
    public ClauseType getClauseType() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String toString() {
        Stream<String> queryPatternStream = patternExpressions.stream().map(QueryPatternExpr::toString);
        return matchType + " " + queryPatternStream.collect(Collectors.joining("\n"));
    }
}
