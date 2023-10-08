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
package org.apache.asterix.graphix.lang.visitor.base;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.mapping.EdgeConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.mapping.VertexConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public interface IGraphixLangVisitor<R, T> extends ILangVisitor<R, T> {
    R visit(LetGraphClause lgc, T arg) throws CompilationException;

    R visit(GraphConstructor gc, T arg) throws CompilationException;

    R visit(EdgeConstructor ee, T arg) throws CompilationException;

    R visit(VertexConstructor ve, T arg) throws CompilationException;

    R visit(CreateGraphStatement cgs, T arg) throws CompilationException;

    R visit(GraphElementDeclaration ged, T arg) throws CompilationException;

    R visit(GraphDropStatement gds, T arg) throws CompilationException;

    R visit(FromGraphClause fgc, T arg) throws CompilationException;

    R visit(FromGraphTerm fgt, T arg) throws CompilationException;

    R visit(MatchClause mc, T arg) throws CompilationException;

    R visit(QueryPatternExpr qpe, T arg) throws CompilationException;

    R visit(PathPatternExpr ppe, T arg) throws CompilationException;

    R visit(EdgePatternExpr epe, T arg) throws CompilationException;

    R visit(VertexPatternExpr vpe, T arg) throws CompilationException;
}
