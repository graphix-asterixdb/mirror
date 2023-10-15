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
package org.apache.asterix.graphix.lang.rewrite;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.compiler.option.CompilationAddContextOption;
import org.apache.asterix.graphix.algebra.compiler.option.EvaluationMinimizeJoinsOption;
import org.apache.asterix.graphix.algebra.compiler.option.EvaluationPreferIndexNLOption;
import org.apache.asterix.graphix.algebra.compiler.option.IGraphixCompilerOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsPatternOption;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Wrapper class for {@link LangRewritingContext} and for Graphix specific rewriting.
 */
public class GraphixRewritingContext extends LangRewritingContext {
    private final Map<String, Pair<VariableExpr, Integer>> copyLineage = new HashMap<>();

    // For applications that need additional context about our query patterns in the response.
    private final IResponsePrinter responsePrinter;

    public GraphixRewritingContext(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions,
            List<ViewDecl> declaredViews, IWarningCollector warningCollector, int varCounter,
            IResponsePrinter responsePrinter) {
        super(metadataProvider, declaredFunctions, declaredViews, warningCollector, varCounter);
        this.responsePrinter = Objects.requireNonNull(responsePrinter);
    }

    public IResponsePrinter getResponsePrinter() {
        return responsePrinter;
    }

    public VariableExpr getGraphixVariableCopy(VariableExpr existingVariable) {
        VariableExpr graphixVariableSource = getGraphixVariableSource(existingVariable);
        VarIdentifier varIdentifier =
                (graphixVariableSource == null) ? existingVariable.getVar() : graphixVariableSource.getVar();
        String variableName = SqlppVariableUtil.toUserDefinedVariableName(varIdentifier).getValue();
        if (copyLineage.containsKey(variableName)) {
            copyLineage.get(variableName).second = copyLineage.get(variableName).second + 1;
        } else {
            copyLineage.put(variableName, new Pair<>(existingVariable, 1));
        }
        int currentCount = copyLineage.get(variableName).second;

        // Our copy is a unique variable (w.r.t. SQL++), but we will manage these associations ourselves.
        VarIdentifier identifierCopy = new VarIdentifier(String.format("COPY_OF(%s,%s)", variableName, currentCount));
        VariableExpr variableCopy = new VariableExpr(identifierCopy);
        variableCopy.setSourceLocation(existingVariable.getSourceLocation());
        return variableCopy;
    }

    public VariableExpr getGraphixVariableSource(VariableExpr copiedVariable) {
        final Pattern variableNamePattern = Pattern.compile("COPY_OF\\((#?[A-Za-z0-9_]+),[0-9]+\\)");
        String copyName = SqlppVariableUtil.toUserDefinedVariableName(copiedVariable.getVar()).getValue();
        Matcher copyMatch = variableNamePattern.matcher(copyName);
        if (!copyMatch.find()) {
            return null;
        }
        String innerVariableName = copyMatch.group(1);
        Pair<VariableExpr, Integer> lineagePair = copyLineage.get(innerVariableName);
        return (lineagePair == null) ? null : lineagePair.first;
    }

    public IGraphixCompilerOption getSetting(String settingName) throws CompilationException {
        IGraphixCompilerOption[] enumValues;
        switch (settingName) {
            case SemanticsNavigationOption.OPTION_KEY_NAME:
                enumValues = SemanticsNavigationOption.values();
                return parseSetting(settingName, enumValues, SemanticsNavigationOption.OPTION_DEFAULT);

            case SemanticsPatternOption.OPTION_KEY_NAME:
                enumValues = SemanticsPatternOption.values();
                return parseSetting(settingName, enumValues, SemanticsPatternOption.OPTION_DEFAULT);

            case EvaluationMinimizeJoinsOption.OPTION_KEY_NAME:
                enumValues = EvaluationMinimizeJoinsOption.values();
                return parseSetting(settingName, enumValues, EvaluationMinimizeJoinsOption.OPTION_DEFAULT);

            case EvaluationPreferIndexNLOption.OPTION_KEY_NAME:
                enumValues = EvaluationPreferIndexNLOption.values();
                return parseSetting(settingName, enumValues, EvaluationPreferIndexNLOption.OPTION_DEFAULT);

            case CompilationAddContextOption.OPTION_KEY_NAME:
                enumValues = CompilationAddContextOption.values();
                return parseSetting(settingName, enumValues, CompilationAddContextOption.OPTION_DEFAULT);

            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "Illegal setting requested!");
        }
    }

    private IGraphixCompilerOption parseSetting(String settingName, IGraphixCompilerOption[] settingValues,
            IGraphixCompilerOption defaultValue) throws CompilationException {
        Object metadataConfigValue = getMetadataProvider().getConfig().get(settingName);
        if (metadataConfigValue != null) {
            String configValueString = ((String) metadataConfigValue).toLowerCase(Locale.ROOT);
            return Stream.of(settingValues).filter(o -> o.getOptionValue().equals(configValueString)).findFirst()
                    .orElseThrow(() -> new CompilationException(ErrorCode.PARAMETER_NO_VALUE, configValueString));
        }
        return defaultValue;
    }
}
