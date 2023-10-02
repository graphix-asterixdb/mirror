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
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map, catchError } from 'rxjs/operators';
import { environment } from '../../../environments/environment';

/*
* location.host gives the origin hostname
* since we have to connect to the query-service api in the origin host
*/
/* Using local proxy if webpack and development */
var AsterixRestApiUrl = '/query-service';

var AsterixDeleteApiUrl = '/admin/requests/running';

if (environment.production) {
    var locationHost =  self.location.host.split(':')
    AsterixRestApiUrl = 'http://' + locationHost[0] + ':19002/query/service';
    AsterixDeleteApiUrl = 'http://' + locationHost[0] + ':19002/admin/requests/running';
}

/*
* SQL query service using AsterixDB REST API /query/service
*/
@Injectable()
export class SQLService {
    defaultPlanFormat='JSON';
    defaultOutputFormat='JSON';
    defaultClientContextID='default';

    /*
    * SQLQueryService constructor using
    * HttpClient from Angular 5
    */
    constructor(private http: HttpClient) {}

     /*
     * sends a select sql++ q100px return all the dataverses
    * from AsterixDB Metadata100px
     */
    selectDataverses() : Observable<any> {
         let query = "SELECT VALUE dv FROM Metadata.`Dataverse` dv"
         return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    /*
    * sends a select sql++ query to return all the datasets
    * from AsterixDB Metadata
    */
    selectDatasets() : Observable<any> {
        let query = "SELECT VALUE ds FROM Metadata.`Dataset` ds"
        return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    /*
    * sends a select sql++ query to sample the passed in dataset.
     */
    sampleDataset(dataset: string) : Observable<any> {
        let query = "SELECT * FROM " + dataset + " LIMIT 1;"
        return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    /*
    * sends a select sql++ query to return all the datatypes
    * from AsterixDB Metadata
    */
    selectDatatypes() : Observable<any> {
        let query = "SELECT VALUE dt FROM Metadata.`Datatype` dt"
        return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    /*
    * sends a select sql++ query to return all the indexes
    * from AsterixDB Metadata
    */
    selectIndexes() : Observable<any> {
        let query = "SELECT VALUE ix FROM Metadata.`Index` ix"
        return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    selectFunctions() : Observable<any> {
      let query = "SELECT VALUE fn FROM Metadata.`Function` fn"
      return this.executeSQLQuery(query, this.defaultPlanFormat, this.defaultOutputFormat, this.defaultClientContextID);
    }

    /*
    * creates a sql++ ddl query to create a Dataverse
    * from AsterixDB Metadata
    */
    createDataverse(dataverse: string) : Observable<any[]> {
        let ddlQuery = "CREATE DATAVERSE " + dataverse + ";";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to drop a Dataverse
    * from AsterixDB Metadata
    */
    dropDataverse(dataverse: string) : Observable<any[]> {
        let ddlQuery = "DROP DATAVERSE " + dataverse; // " IF EXISTS;";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to create a Dataset
    * from AsterixDB Metadata
    */
    createDataset(dataset: string) : Observable<any[]> {
        let ddlQuery = "CREATE DATASET " + dataset + ";";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to drop a Dataset
    * from AsterixDB Metadata
    */
    dropDataset(dataset: string) : Observable<any[]> {
        let ddlQuery = "DROP DATASET " + dataset; //" IF EXISTS;";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to create a Datatype
    * from AsterixDB Metadata
    */
    createDatatype(datatype: string) : Observable<any[]> {
        let ddlQuery = "CREATE DATATYPE " + datatype + ";";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to drop a Datatype
    * from AsterixDB Metadata
    */
    dropDatatype(datatype: string) : Observable<any[]> {
        let ddlQuery = "DROP TYPE " + datatype; //" IF EXISTS;";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to create a Index
    * from AsterixDB Metadata
    */
    createIndex(index: string) : Observable<any[]> {
        let ddlQuery = "CREATE INDEX " + index + ";";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * creates a sql++ ddl query to drop a Index
    * from AsterixDB Metadata
    */
    dropIndex(index: string) : Observable<any[]> {
        let ddlQuery = "DROP INDEX " + index; // + " IF EXISTS;";
        return this.executeDDLSQLQuery(ddlQuery);
    }

    /*
    * Executes a sql++ ddl query against AsterixDB
    * response is a JSON object with following structure:
          metrics: Metrics;
          requestId: string;
          results: any[];
          signature: string;
          status: string;
    */
    executeDDLSQLQuery(ddlQuery: string): Observable<any> {
    const apiUrl = AsterixRestApiUrl;
        return this.http.post(apiUrl, {statement: ddlQuery})
          .catch((error: any) => this.handleExecuteQueryError(error));
    }

    /*
    * Executes a sql++ query against AsterixDB
    * response is a JSON object with following structure:
          metrics: Metrics;
          requestId: string;
          results: any[];
          signature: string;
          status: string;
          login(username: string, password: string): Observable<boolean>
    */
    executeSQLQuery(query: string, planFormat: string, outputFormat: string, clientContextID: string): Observable<any> {
        const apiUrl = AsterixRestApiUrl;
        let headers = new HttpHeaders();

        if (outputFormat == 'CSV_header') {
          headers = new HttpHeaders({"Accept": "text/csv; header=present", "Content-Type": "application/json"});
          outputFormat = 'CSV';
        }
        else if (outputFormat == 'CSV') {
          headers = new HttpHeaders({"Accept": "text/csv; header=absent", "Content-Type": "application/json"});
        }
        else
          headers = headers.append('Content-Type', 'application/json');

        let options = ({ headers: headers });

        let body = {
          statement: query,
          'logical-plan': true,
          'optimized-logical-plan': true,
          'plan-format': planFormat,
          'max-warnings': Number.MAX_SAFE_INTEGER
        }

        if (clientContextID != 'default') {
          body['client_context_id'] = clientContextID;
        }

        return this.http.post(apiUrl, body, options)
            .pipe(map((response: Response) => { return response; }),
                  catchError((error: any) => this.handleExecuteQueryError(error)))


    }

    /*
    * Cancels a sql++ query
     */
    cancelSQLQuery(clientContextID: string): Observable<any> {
      let url = AsterixDeleteApiUrl;
      let headers = new HttpHeaders();

      url += `?client_context_id=${clientContextID}`;
      headers = headers.append('Content-Type', 'application/json');

      let options = ({ headers: headers });

      return this.http.delete(url, options)
        .pipe(map((response: Response) => {return response; }),
          catchError((error: any) => this.handleDeleteQueryError(error))
        )
    }

    /*
    * AsterixDB query-service API raises HTTP errors if the sql++ query has some
    * syntax error, or some elements in the query are not found
    * this function extract the error JSON object with the relevant information
        response is a JSON object with following structure:
          metrics: Metrics;
          requestId: string;
          errors: any[];
          signature: string;
          status: string;
    */
    private handleExecuteQueryError(error: any): Promise<any> {
        console.log('executeQueryError:')
        console.log(error);
        return Promise.reject(error.error || error);
    }

    private handleDeleteQueryError(error: any): Promise<any> {
      console.log('deleteQueryError:')
      console.log(error);
      return Promise.reject(error.error || error);
    }
}
