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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import static org.apache.asterix.graphix.extension.GraphixMetadataExtension.getGraph;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.resolve.schema.SchemaTable;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class SchemaTableCreationVisitor extends SuperPatternCreationVisitor {
    private final Map<GraphIdentifier, SchemaTable> schemaTableMap = new LinkedHashMap<>();

    public SchemaTableCreationVisitor(GraphixRewritingContext graphixRewritingContext) {
        super(graphixRewritingContext);
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        GraphIdentifier graphIdentifier = fgt.createGraphIdentifier(metadataProvider);
        scopeChecker.createNewScope();
        SchemaTable schemaTable;

        GraphConstructor graphConstructor = getGraphConstructor(graphIdentifier);
        if (graphConstructor != null) {
            // If our graph has been defined locally (this will take precedence), use this.
            schemaTable = new SchemaTable(graphConstructor, graphIdentifier);

        } else {
            // Otherwise, we will need to search our metadata.
            DataverseName defaultDataverse = metadataProvider.getDefaultDataverseName();
            DataverseName dataverseName = Objects.requireNonNullElse(fgt.getDataverseName(), defaultDataverse);
            Identifier graphName = fgt.getGraphName();
            try {
                MetadataTransactionContext metadataTxnContext = metadataProvider.getMetadataTxnContext();
                Graph graphFromMetadata = getGraph(metadataTxnContext, dataverseName, graphName.getValue());
                if (graphFromMetadata == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }
                schemaTable = new SchemaTable(graphFromMetadata);

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fgt.getSourceLocation(),
                        "Graph " + graphName.getValue() + " does not exist.");
            }
        }

        // Update our map.
        schemaTableMap.put(graphIdentifier, schemaTable);
        return super.visit(fgt, arg);
    }

    public Map<GraphIdentifier, SchemaTable> getSchemaTableMap() {
        return schemaTableMap;
    }
}
