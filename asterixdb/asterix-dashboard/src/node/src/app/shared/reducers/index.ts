/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import * as fromDataverse from './dataverse.reducer';
import * as fromDataset from './dataset.reducer';
import * as fromDatatype from './datatype.reducer';
import * as fromIndex from './index.reducer';
import * as fromFunction from './function.reducer';
import * as fromQuery from './query.reducer';
import * as fromQueryMetadata from './query-metadata.reducer';
import * as fromAppState from './app.reducer';
import * as fromCancel from './cancel.reducer';

/*
** Global Interfaces store/state
*/
export interface ModelState {
    dataverse: fromDataverse.State,
    dataset: fromDataset.State,
    datatype: fromDatatype.State,
    index: fromIndex.State,
    functions: fromFunction.State,
    sqlQuery: fromQuery.State,
    sqlMetadataQuery: fromQueryMetadata.State,
    appState: fromAppState.State,
}

/*
** Global Reducers configuration
*/
export const reducers = {
    dataverse: fromDataverse.dataverseReducer,
    dataset: fromDataset.datasetReducer,
    datatype: fromDatatype.datatypeReducer,
    index: fromIndex.indexReducer,
    functions: fromFunction.functionReducer,
    sqlQuery: fromQuery.sqlReducer,
    cancelQuery: fromCancel.cancelReducer,
    sqlMetadataQuery: fromQueryMetadata.sqlMetadataReducer,
    app: fromAppState.appReducer
};
