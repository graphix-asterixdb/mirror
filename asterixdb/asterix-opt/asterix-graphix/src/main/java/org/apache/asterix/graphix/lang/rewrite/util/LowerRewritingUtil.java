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
package org.apache.asterix.graphix.lang.rewrite.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.hyracks.algebricks.common.utils.Triple;

public final class LowerRewritingUtil {
    public static Expression buildConnectedClauses(List<Expression> clauses, OperatorType connector) {
        switch (clauses.size()) {
            case 0:
                // We must be given a non-empty list.
                throw new IllegalArgumentException("Given empty set of clauses.");

            case 1:
                // We do not need to connect a single clause.
                return clauses.get(0);

            default:
                // Otherwise, connect all non-true clauses.
                return new OperatorExpr(clauses, Collections.nCopies(clauses.size() - 1, connector), false);
        }
    }

    public static Expression buildSingleElementJoin(List<? extends Expression> left, List<? extends Expression> right) {
        if (left.size() != right.size()) {
            throw new IllegalStateException("Left access list size is not equal to the right access list size!");
        }
        List<Expression> joinClauses = new ArrayList<>();
        for (int i = 0; i < right.size(); i++) {
            List<Expression> equalityArgs = List.of(right.get(i), left.get(i));
            OperatorExpr equalityExpr = new OperatorExpr(equalityArgs, List.of(OperatorType.EQ), false);
            joinClauses.add(equalityExpr);
        }
        return buildConnectedClauses(joinClauses, OperatorType.AND);
    }

    public static List<FieldAccessor> buildSingleAccessorList(Expression startingExpr, List<List<String>> fieldNames)
            throws CompilationException {
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        List<FieldAccessor> fieldAccessors = new ArrayList<>();
        for (List<String> nestedField : fieldNames) {
            Expression copiedStartingExpr = (Expression) startingExpr.accept(deepCopyVisitor, null);
            FieldAccessor workingAccessor = new FieldAccessor(copiedStartingExpr, new Identifier(nestedField.get(0)));
            for (String field : nestedField.subList(1, nestedField.size())) {
                workingAccessor = new FieldAccessor(workingAccessor, new Identifier(field));
            }
            fieldAccessors.add(workingAccessor);
        }
        return fieldAccessors;
    }

    public static void remapVariablesInPattern(IPatternConstruct patternConstruct,
            VariableRemapCloneVisitor remapCloneVisitor) throws CompilationException {
        switch (patternConstruct.getPatternType()) {
            case VERTEX_PATTERN:
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
                Expression vertexFilter = vertexDescriptor.getFilterExpr();
                vertexDescriptor.setVariableExpr((VariableExpr) remapCloneVisitor.substitute(vertexVariable));
                if (vertexFilter != null) {
                    vertexDescriptor.setFilterExpr((Expression) remapCloneVisitor.substitute(vertexFilter));
                }
                break;
            case EDGE_PATTERN:
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                Expression edgeFilter = edgeDescriptor.getFilterExpr();
                edgeDescriptor.setVariableExpr((VariableExpr) remapCloneVisitor.substitute(edgeVariable));
                if (edgeFilter != null) {
                    edgeDescriptor.setFilterExpr((Expression) remapCloneVisitor.substitute(edgeFilter));
                }
                remapVariablesInPattern(edgePatternExpr.getLeftVertex(), remapCloneVisitor);
                remapVariablesInPattern(edgePatternExpr.getRightVertex(), remapCloneVisitor);
                break;
            case PATH_PATTERN:
                PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
                VariableExpr pathVariable = pathDescriptor.getVariableExpr();
                pathDescriptor.setVariableExpr((VariableExpr) remapCloneVisitor.substitute(pathVariable));
                remapVariablesInPattern(pathPatternExpr.getLeftVertex(), remapCloneVisitor);
                remapVariablesInPattern(pathPatternExpr.getRightVertex(), remapCloneVisitor);
                for (EdgePatternExpr decomposition : pathPatternExpr.getDecompositions()) {
                    remapVariablesInPattern(decomposition, remapCloneVisitor);
                }
                break;
        }
    }

    public static List<Triple<Expression, Expression, Expression>> extractCommonJoinTerms(
            List<Expression> leftExpressionList, List<Expression> rightExpressionList) {
        List<Triple<Expression, Expression, Expression>> commonExprList = new ArrayList<>();
        for (Expression leftExpression : leftExpressionList) {
            if (leftExpression.getKind() != Expression.Kind.OP_EXPRESSION) {
                continue;
            }
            OperatorExpr leftOpExpr = (OperatorExpr) leftExpression;
            for (Expression rightExpression : rightExpressionList) {
                if (rightExpression.getKind() != Expression.Kind.OP_EXPRESSION) {
                    continue;
                }
                OperatorExpr rightOpExpr = (OperatorExpr) rightExpression;

                // Note: we assume a top-level list of operands.
                Iterator<Expression> leftOperandIterator = leftOpExpr.getExprList().iterator();
                Iterator<Expression> rightOperandIterator = rightOpExpr.getExprList().iterator();
                Iterator<OperatorType> leftOpIterator = leftOpExpr.getOpList().iterator();
                Iterator<OperatorType> rightOpIterator = rightOpExpr.getOpList().iterator();
                while (leftOpIterator.hasNext() && rightOpIterator.hasNext()) {
                    boolean isLeftOpEquals = leftOpIterator.next() == OperatorType.EQ;
                    boolean isRightOpEquals = rightOpIterator.next() == OperatorType.EQ;
                    if (!isLeftOpEquals || !isRightOpEquals) {
                        continue;
                    }
                    Expression leftOperand1 = leftOperandIterator.next();
                    Expression leftOperand2 = leftOperandIterator.next();
                    Expression rightOperand1 = rightOperandIterator.next();
                    Expression rightOperand2 = rightOperandIterator.next();
                    if (Objects.equals(leftOperand1, rightOperand1)) {
                        commonExprList.add(new Triple<>(leftOperand1, leftOperand2, rightOperand2));
                    } else if (Objects.equals(leftOperand1, rightOperand2)) {
                        commonExprList.add(new Triple<>(leftOperand1, leftOperand2, rightOperand1));
                    } else if (Objects.equals(leftOperand2, rightOperand1)) {
                        commonExprList.add(new Triple<>(leftOperand2, leftOperand1, rightOperand2));
                    } else if (Objects.equals(leftOperand2, rightOperand2)) {
                        commonExprList.add(new Triple<>(leftOperand2, leftOperand1, rightOperand1));
                    }
                }
            }
        }
        return commonExprList;
    }

    public static RecordConstructor buildRecordConstructor(List<List<String>> vertexKeyField,
            List<List<String>> otherKeyField, VariableExpr otherVariableExpr) {
        // Build access to our key fields.
        List<FieldAccessor> edgeAccessors = new ArrayList<>();
        for (List<String> edgeFieldName : otherKeyField) {
            VariableExpr edgeVariableCopy = new VariableExpr(otherVariableExpr.getVar());
            edgeAccessors.add(buildFieldAccessor(edgeVariableCopy, edgeFieldName));
        }
        Iterator<FieldAccessor> edgeAccessIterator = edgeAccessors.iterator();

        // Build up our record constructor.
        RecordConstructor finalRecordConstructor = new RecordConstructor(new ArrayList<>());
        for (List<String> vertexFieldName : vertexKeyField) {
            List<FieldBinding> fieldBindings = new ArrayList<>();
            fieldBindings.add(buildFieldBinding(vertexFieldName, edgeAccessIterator.next()));
            mergeRecordConstructors(finalRecordConstructor, new RecordConstructor(fieldBindings));
        }
        return finalRecordConstructor;
    }

    public static RecordConstructor mergeRecordConstructors(RecordConstructor record1, RecordConstructor record2) {
        for (FieldBinding fieldBinding2 : record2.getFbList()) {
            LiteralExpr leftExpr2 = (LiteralExpr) fieldBinding2.getLeftExpr();

            // Do our record constructors share keys? Recurse.
            Optional<FieldBinding> optionalBinding1 =
                    record1.getFbList().stream().filter(b -> b.getLeftExpr().equals(leftExpr2)).findAny();
            if (optionalBinding1.isPresent()) {
                FieldBinding fieldBinding1 = optionalBinding1.get();
                RecordConstructor rightRecord1 = (RecordConstructor) fieldBinding1.getRightExpr();
                RecordConstructor rightRecord2 = (RecordConstructor) fieldBinding2.getRightExpr();
                fieldBinding1.setRightExpr(mergeRecordConstructors(rightRecord1, rightRecord2));
            } else {
                record1.getFbList().add(fieldBinding2);
            }
        }
        return record1;
    }

    public static FieldBinding buildFieldBinding(List<String> fieldName, Expression rightExpression) {
        switch (fieldName.size()) {
            case 0:
                throw new IllegalStateException();

            case 1:
                return new FieldBinding(new LiteralExpr(new StringLiteral(fieldName.get(0))), rightExpression);

            default:
                List<String> subFieldName = fieldName.subList(1, fieldName.size());
                List<FieldBinding> nestedFieldBindings = new ArrayList<>();
                nestedFieldBindings.add(buildFieldBinding(subFieldName, rightExpression));
                RecordConstructor recordConstructor = new RecordConstructor(nestedFieldBindings);
                LiteralExpr leftExpression = new LiteralExpr(new StringLiteral(fieldName.get(0)));
                return new FieldBinding(leftExpression, recordConstructor);
        }
    }

    public static FieldAccessor buildFieldAccessor(VariableExpr variableExpr, List<String> fieldName) {
        switch (fieldName.size()) {
            case 0:
                throw new IllegalStateException();

            case 1:
                return new FieldAccessor(variableExpr, new Identifier(fieldName.get(0)));

            default:
                List<String> subFieldName = fieldName.subList(0, fieldName.size() - 1);
                FieldAccessor innerFieldAccessor = buildFieldAccessor(variableExpr, subFieldName);
                return new FieldAccessor(innerFieldAccessor, new Identifier(fieldName.get(fieldName.size() - 1)));
        }
    }
}
