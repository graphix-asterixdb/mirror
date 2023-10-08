/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.GraphixFunctionResolver;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppFunctionCallResolverVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Resolve all function calls, while accounting for Graphix specific functions. This is meant to replace the class
 * {@link org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppFunctionCallResolverVisitor}, and should contain all
 * the functionality exposed there.
 * <p>
 * This rewrite does not depend on any other rewrite.
 */
public class FunctionResolutionVisitor extends AbstractGraphixQueryVisitor {
    private final SqlppFunctionCallResolverVisitor sqlppResolver;
    private final GraphixFunctionResolver graphixResolver;
    private final boolean allowNonStoredUdfCalls;

    public FunctionResolutionVisitor(GraphixRewritingContext graphixRewritingContext, boolean allowNonStoredUDFCalls) {
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        Map<FunctionSignature, FunctionDecl> declaredFunctions = graphixRewritingContext.getDeclaredFunctions();
        this.graphixResolver = new GraphixFunctionResolver(metadataProvider, declaredFunctions);
        this.sqlppResolver = new SqlppFunctionCallResolverVisitor(graphixRewritingContext, allowNonStoredUDFCalls) {
            @Override
            public Expression visit(FromClause fc, ILangExpression arg) throws CompilationException {
                // We will skip over our FROM-GRAPH-TERMs here.
                if (fc instanceof FromGraphClause) {
                    FromGraphClause fromGraphClause = (FromGraphClause) fc;
                    for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
                        if (fromTerm instanceof FromGraphTerm) {
                            continue;
                        }
                        fromTerm.accept(this, arg);
                    }
                    return null;
                }
                return fc.accept(this, arg);
            }
        };
        this.allowNonStoredUdfCalls = allowNonStoredUDFCalls;
    }

    @Override
    public Expression visit(CallExpr ce, ILangExpression arg) throws CompilationException {
        FunctionSignature functionSignature = graphixResolver.resolve(ce, allowNonStoredUdfCalls);
        if (functionSignature != null) {
            ce.setFunctionSignature(functionSignature);
            return super.visit(ce, arg);
        }
        return sqlppResolver.visit(ce, arg);
    }

    @Override
    public Expression visit(WindowExpression we, ILangExpression arg) throws CompilationException {
        // Delegate WINDOW function resolution to our SQL++ resolver.
        return sqlppResolver.visit(we, arg);
    }
}
