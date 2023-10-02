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
import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { AppComponent } from './app.component';
import { AppEffects } from './shared/effects/app.effects';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule } from '@angular/common/http';
import { EffectsModule } from '@ngrx/effects';
import { DataverseEffects } from './shared/effects/dataverse.effects';
import { DatasetEffects } from './shared/effects/dataset.effects';
import { DatatypeEffects } from './shared/effects/datatype.effects';
import { IndexEffects } from './shared/effects/index.effects';
import { FunctionEffects } from "./shared/effects/function.effects";
import { SQLQueryEffects } from './shared/effects/query.effects';
import { AppBarComponent }  from './dashboard/appbar.component';
import { DialogMetadataInspector, MetadataComponent }  from './dashboard/query/metadata.component';
import { QueryContainerComponent }  from './dashboard/query/query-container.component';
import { InputQueryComponent }  from './dashboard/query/input.component';
import { QueryOutputComponent }  from './dashboard/query/output.component';
import { AppTabComponent }  from './dashboard/apptab.component';
import { reducers } from './shared/reducers';
import { SQLService } from './shared/services/async-query.service'
import { FormsModule } from '@angular/forms';
import { MaterialModule } from './material.module';
import { StoreModule,  } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { NgxGraphModule } from "@swimlane/ngx-graph";
import { NgxChartsModule } from "@swimlane/ngx-charts";
import { PlanViewerComponent } from "./dashboard/query/plan-viewer.component";
import { TreeNodeComponent } from './dashboard/query/tree-node.component';
import { DialogExportPicker, TreeViewComponent } from './dashboard/query/tree-view.component';
import {SQLCancelEffects} from "./shared/effects/cancel.effects";

@NgModule({
    declarations: [
        AppComponent,
        AppBarComponent,
        InputQueryComponent,
        QueryOutputComponent,
        MetadataComponent,
        QueryContainerComponent,
        AppTabComponent,
        DialogMetadataInspector,
        PlanViewerComponent,
        TreeNodeComponent,
        DialogExportPicker,
        TreeViewComponent,
    ],
    imports: [
        FormsModule,
        BrowserModule,
        BrowserAnimationsModule,
        EffectsModule.forRoot([AppEffects, DataverseEffects, DatasetEffects, DatatypeEffects, IndexEffects, FunctionEffects, SQLQueryEffects, SQLCancelEffects]),
        HttpClientModule,
        MaterialModule,
        StoreModule.forRoot(reducers, {
          runtimeChecks: {
            strictStateImmutability: false,
            strictActionImmutability: false,
            strictStateSerializability: false,
            strictActionSerializability: false,
          },
        }),
        StoreDevtoolsModule.instrument({
            maxAge: 10
        }),
        NgxGraphModule,
        NgxChartsModule
    ],
    schemas: [CUSTOM_ELEMENTS_SCHEMA],
    entryComponents: [
        DialogMetadataInspector,
        DialogExportPicker,
    ],
    providers: [SQLService],
    bootstrap: [AppComponent]
})
export class AppModule {}
