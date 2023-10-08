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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.rewrite.print.GraphixASTPrintVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ClauseSequence implements Iterable<AbstractClause> {
    private final List<LetClause> representativeVertexBindings = new ArrayList<>();
    private final List<LetClause> representativeEdgeBindings = new ArrayList<>();
    private final List<LetClause> representativePathBindings = new ArrayList<>();
    private final List<AbstractClause> nonRepresentativeClauses = new LinkedList<>();
    private final List<AbstractBinaryCorrelateClause> userCorrelateClauses = new ArrayList<>();

    // We need to know which non-representative-bindings come from a LEFT-MATCH clause.
    private final Set<LetClause> fromLeftMatchBindings = new HashSet<>();

    // Note: this class is append-only. We specify deletions using the anti-matter-lists / flags below.
    private final Set<AbstractClause> excludedClauses = new HashSet<>();

    public void addNonRepresentativeClause(AbstractClause clause) {
        switch (clause.getClauseType()) {
            case JOIN_CLAUSE:
                Stream<Clause.ClauseType> c = nonRepresentativeClauses.stream().map(AbstractClause::getClauseType);
                if (c.noneMatch(Clause.ClauseType.JOIN_CLAUSE::equals) && !nonRepresentativeClauses.isEmpty()) {
                    // If this is our first JOIN clause, make sure we move this to the front.
                    nonRepresentativeClauses.add(0, clause);
                    break;
                }
            case UNNEST_CLAUSE:
            case LET_CLAUSE:
            case WHERE_CLAUSE:
                nonRepresentativeClauses.add(clause);
                break;

            case EXTENSION:
                if (clause instanceof BranchingClause || clause instanceof LoopingClause) {
                    nonRepresentativeClauses.add(clause);
                    break;
                }

            default:
                throw new IllegalStateException("Illegal clause inserted into the lower clause list!");
        }
    }

    // Note: we allow our vertices to be rebound, but not our paths and edges.
    public void setVertexBinding(VariableExpr bindingVar, Expression boundExpression) {
        Optional<LetClause> existingBinding =
                representativeVertexBindings.stream().filter(v -> v.getVarExpr().equals(bindingVar)).findAny();
        if (existingBinding.isPresent()) {
            existingBinding.get().setBindingExpr(boundExpression);
        } else {
            representativeVertexBindings.add(new LetClause(bindingVar, boundExpression));
        }
    }

    public void addEdgeBinding(VariableExpr bindingVar, Expression boundExpression) {
        if (representativeEdgeBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new IllegalStateException("Duplicate edge binding found!");
        }
        representativeEdgeBindings.add(new LetClause(bindingVar, boundExpression));
    }

    public void addPathBinding(VariableExpr bindingVar, Expression boundExpression) {
        if (representativePathBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new IllegalStateException("Duplicate path binding found!");
        }
        representativePathBindings.add(new LetClause(bindingVar, boundExpression));
    }

    public void addUserDefinedCorrelateClause(AbstractBinaryCorrelateClause correlateClause) {
        userCorrelateClauses.add(correlateClause);
    }

    public void addLeftMatchBinding(VariableExpr bindingVar, Expression boundExpression) {
        if (fromLeftMatchBindings.stream().anyMatch(v -> v.getVarExpr().equals(bindingVar))) {
            throw new IllegalStateException("Duplicate LEFT-MATCH binding found!");
        }
        LetClause letClause = new LetClause(bindingVar, boundExpression);
        fromLeftMatchBindings.add(letClause);
        addNonRepresentativeClause(letClause);
    }

    public void removeClause(AbstractClause abstractClause) {
        for (AbstractClause clause : nonRepresentativeClauses) {
            if (clause.equals(abstractClause)) {
                excludedClauses.add(clause);
                return;
            }
        }
        for (LetClause clause : representativeVertexBindings) {
            if (clause.equals(abstractClause)) {
                excludedClauses.add(clause);
                return;
            }
        }
        for (LetClause clause : representativeEdgeBindings) {
            if (clause.equals(abstractClause)) {
                excludedClauses.add(clause);
                return;
            }
        }
        for (LetClause clause : representativePathBindings) {
            if (clause.equals(abstractClause)) {
                excludedClauses.add(clause);
                return;
            }
        }
        throw new IllegalStateException("Trying to remove a non-existent CLAUSE!");
    }

    public List<LetClause> getRepresentativeVertexBindings() {
        return Collections.unmodifiableList(representativeVertexBindings);
    }

    public List<LetClause> getRepresentativeEdgeBindings() {
        return Collections.unmodifiableList(representativeEdgeBindings);
    }

    public List<LetClause> getRepresentativePathBindings() {
        return Collections.unmodifiableList(representativePathBindings);
    }

    public List<AbstractClause> getNonRepresentativeClauses() {
        return Collections.unmodifiableList(nonRepresentativeClauses);
    }

    public Set<LetClause> getFromLeftMatchBindings() {
        return Collections.unmodifiableSet(fromLeftMatchBindings);
    }

    public SourceLocation getSourceLocation() {
        return iterator().next().getSourceLocation();
    }

    @Override
    public Iterator<AbstractClause> iterator() {
        IteratorChain<AbstractClause> iteratorChain = new IteratorChain<>();
        Function<AbstractClause, Boolean> clauseFilter = c -> !excludedClauses.contains(c);
        iteratorChain.addIterator(nonRepresentativeClauses.stream().filter(clauseFilter::apply).iterator());
        iteratorChain.addIterator(representativeVertexBindings.stream().filter(clauseFilter::apply).iterator());
        iteratorChain.addIterator(representativeEdgeBindings.stream().filter(clauseFilter::apply).iterator());
        iteratorChain.addIterator(representativePathBindings.stream().filter(clauseFilter::apply).iterator());
        iteratorChain.addIterator(userCorrelateClauses.iterator());
        return iteratorChain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClauseSequence)) {
            return false;
        }
        ClauseSequence that = (ClauseSequence) o;
        return Objects.equals(IterableUtils.toList(this), IterableUtils.toList(that));
    }

    @Override
    public int hashCode() {
        return Objects.hash(IterableUtils.toList(this));
    }

    // TODO (GLENN): It would be nice to clean up this method in the future, but this method is only used for debugging.
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Consumer<ILangExpression> expressionASTPrinter = e -> {
            try {
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                e.accept(new GraphixASTPrintVisitor(printWriter), 0);
                sb.append(stringWriter.toString().trim().replaceAll("[\\s\\n]+", " "));

            } catch (CompilationException ex) {
                throw new RuntimeException(ex);
            }
        };

        // Iterate through all of our clauses.
        sb.append("SEQUENCE OF\n");
        int index = 0;
        for (AbstractClause abstractClause : this) {
            if (index++ > 0) {
                sb.append("\n");
            }
            sb.append("- ");
            switch (abstractClause.getClauseType()) {
                case LET_CLAUSE:
                    LetClause letClause = (LetClause) abstractClause;
                    sb.append("LET ").append(letClause.getVarExpr()).append(" <- ");
                    expressionASTPrinter.accept(letClause.getBindingExpr());
                    break;
                case WHERE_CLAUSE:
                    WhereClause whereClause = (WhereClause) abstractClause;
                    sb.append("WHERE ");
                    expressionASTPrinter.accept(whereClause.getWhereExpr());
                    break;
                case JOIN_CLAUSE:
                    JoinClause joinClause = (JoinClause) abstractClause;
                    if (joinClause.getJoinType() == JoinType.LEFTOUTER) {
                        sb.append("LEFT ");
                    }
                    sb.append("JOIN ");
                    if (joinClause.getRightExpression() instanceof SelectExpression) {
                        SelectExpression selectExpression = (SelectExpression) joinClause.getRightExpression();
                        SetOperationInput leftInput = selectExpression.getSelectSetOperation().getLeftInput();
                        if (leftInput.selectBlock()) {
                            SelectBlock selectBlock = leftInput.getSelectBlock();
                            if (selectBlock.hasFromClause() && selectBlock.getFromClause() instanceof FromGraphClause) {
                                FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
                                boolean isBreak = false;
                                for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
                                    if (fromTerm instanceof FromGraphTerm) {
                                        FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                                        if (fromGraphTerm.getLowerClause() != null) {
                                            String lowerClauseString = fromGraphTerm.getLowerClause().toString();
                                            sb.append(lowerClauseString.replaceAll("\n", "\n    ")).append("\n  AS ");
                                            sb.append(joinClause.getRightVariable()).append("\n  ON ");
                                            expressionASTPrinter.accept(joinClause.getConditionExpression());
                                            isBreak = true;
                                            break;
                                        }
                                    } else {
                                        expressionASTPrinter.accept(fromTerm);
                                    }
                                }
                                if (isBreak) {
                                    break;
                                }
                            }
                        }
                    }
                    expressionASTPrinter.accept(joinClause.getRightExpression());
                    sb.append("\n  AS ").append(joinClause.getRightVariable()).append("\n  ON ");
                    expressionASTPrinter.accept(joinClause.getConditionExpression());
                    break;
                case UNNEST_CLAUSE:
                    UnnestClause unnestClause = (UnnestClause) abstractClause;
                    sb.append("UNNEST ");
                    expressionASTPrinter.accept(unnestClause.getRightExpression());
                    sb.append("\n  AS ").append(unnestClause.getRightVariable());
                    break;
                default:
                    throw new IllegalStateException("Unexpected clause!");
            }
        }
        return sb.toString();
    }
}
