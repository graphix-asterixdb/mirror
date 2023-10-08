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
package org.apache.asterix.graphix.lang.rewrite.scope;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.hyracks.algebricks.common.utils.Triple;

public class ScopedCollection<T> {
    private final List<Triple<T, Identifier, Scope>> elementScopeList = new ArrayList<>();

    public void addElement(T element, Identifier symbol, Scope scope) {
        elementScopeList.add(new Triple<>(element, symbol, scope));
    }

    public List<T> getElementsInScope(Scope scope) {
        List<T> resultElements = new ArrayList<>();
        for (Triple<T, Identifier, Scope> elementScopeTriple : elementScopeList) {
            Scope associatedScope = elementScopeTriple.third;
            Identifier associatedId = elementScopeTriple.second;
            T associatedElement = elementScopeTriple.first;
            if (Objects.equals(associatedScope, scope)) {
                resultElements.add(associatedElement);

            } else {
                // The associated scope may have been merged upward.
                Scope ancestorScope = findLowestCommonAncestor(scope, associatedScope);
                if (ancestorScope != null && ancestorScope.findSymbol(associatedId.getValue()) != null) {
                    resultElements.add(associatedElement);
                }
            }
        }
        return resultElements;
    }

    private Scope findLowestCommonAncestor(Scope scope1, Scope scope2) {
        // First, determine the depth from the root for each scope.
        int depth1 = findDepthFromRoot(scope1);
        int depth2 = findDepthFromRoot(scope2);

        // Next, we will move deeper pointer first.
        Scope workingScope1 = scope1;
        Scope workingScope2 = scope2;
        while (workingScope1 != null && workingScope2 != null && !Objects.equals(workingScope1, workingScope2)) {
            if (depth1 < depth2) {
                // Advance our second pointer.
                workingScope2 = workingScope2.getParentScope();
                depth2--;

            } else {
                // Advance our first pointer.
                workingScope1 = workingScope1.getParentScope();
                depth1--;
            }
        }

        // We have either found a common ancestor, or traversed the shorter of the two trees.
        if (Objects.equals(workingScope1, workingScope2)) {
            return workingScope1;

        } else {
            // There exists no common ancestor.
            return null;
        }
    }

    private int findDepthFromRoot(Scope scope) {
        return (scope.getParentScope() == null) ? 0 : (findDepthFromRoot(scope.getParentScope()) + 1);
    }
}
