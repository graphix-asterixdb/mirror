# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import abc
import multiprocessing
import subprocess
import dataclasses
import datetime
import json
import logging
import logging.config
import email.message
import os
import random
import time
import base64
import timeit
import typing
import dotenv
import uuid
import re

import numpy
import pandas
import google_auth_oauthlib.flow
import google.auth.transport.requests
import google.oauth2.credentials
import googleapiclient.discovery
import googleapiclient.errors

LOGGER = logging.getLogger(__name__)
FORMATTER = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)

@dataclasses.dataclass
class ClientQuerySpecification:
    parameter_files: typing.List[str]
    query_file: str
    repeat: int
    timeout: int

@dataclasses.dataclass
class ClientQuery:
    parameter_file: str
    query_file: str
    timeout: int

@dataclasses.dataclass
class ExecutionStatistics:
    is_ended_in_error: bool = False
    total_executions: int = 0
    start_time: datetime.datetime = None
    end_time: datetime.datetime = None
    total_running_time: datetime.timedelta = None
    queries_executed: typing.List[dict] = dataclasses.field(default_factory=list)

class AbstractBenchmarkClient(abc.ABC):
    """ Base class which facilitates our benchmark client. """

    def __init__(self, query_files: typing.Iterable[str], parameter_files: typing.Iterable[str], result_file: str,
                 log_file: str, restart_command: str, specification: typing.List[ClientQuerySpecification],
                 retries: int, random_seed: int, is_notify: bool, is_debug: bool, config: typing.Dict):
        """
        :param query_files: Iterable of strings that correspond to query filenames.
        :param parameter_files: Iterable of strings that correspond to parameter filenames.
        :param result_file: File to write benchmark results to (as JSON lines).
        :param log_file: File to write log entries to.
        :param restart_command: Space separated string indicating a bash command to restart the database.
        :param specification: List of queries to generate a benchmark for.
        :param retries: Number of times to retry a failed query.
        :param random_seed: Random seed used for generating our workload.
        :param is_notify: Flag that determines whether we notify our user by email when the benchmark is done.
        :param is_debug: Flag that determines whether we are in debug mode or not.
        :param config: Configuration of the benchmark client.
        """
        self.results_fp = open(result_file, 'w')
        self.parameter_files = parameter_files
        self.query_files = query_files
        self.restart_command = restart_command
        self.is_debug = is_debug
        self.retries = retries
        dotenv.load_dotenv()

        # We will log to the console and to a file.
        LOGGER.setLevel(logging.DEBUG if is_debug else logging.INFO)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(FORMATTER)
        LOGGER.addHandler(fh)

        # Connect to the gmail service.
        self.is_notify = is_notify
        if self.is_notify:
            self.create_gmail_service()

        # Define our execution specific state.
        self.execution_statistics = ExecutionStatistics()
        self.client_class = self.__class__.__name__
        self.execution_id = str(uuid.uuid4())
        self.random_seed = random_seed

        # ...and end by displaying our configuration.
        self.config = config
        self.specification = specification
        LOGGER.info(f'Using the following configuration: {self.config}')

    @staticmethod
    def call_subprocess(command, is_log=True):
        subprocess_pipe = subprocess.Popen(
            ['/bin/bash'] + command.split(' '),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        resultant = ''
        for stdout_line in iter(subprocess_pipe.stdout.readline, ""):
            if stdout_line.strip() != '':
                resultant += stdout_line
                for line in resultant.strip().split('\n'):
                    if is_log:
                        LOGGER.debug(line)
        subprocess_pipe.stdout.close()
        return resultant

    def log_execution(self, results):
        results['logTime'] = str(datetime.datetime.now())
        results['executionID'] = self.execution_id
        results['clientClass'] = self.client_class
        results['config'] = self.config

        # To the results file.
        LOGGER.debug('Recording execution record to disk.')
        json.dump(results, self.results_fp, default=str)
        self.results_fp.write('\n')

        # To the console.
        LOGGER.debug('Writing execution record to console.')
        LOGGER.debug(json.dumps(results, indent=2, sort_keys=True, default=str))

    @staticmethod
    def create_gmail_service():
        scopes = ['https://www.googleapis.com/auth/gmail.send']
        credentials = None
        if os.path.exists(os.getenv('GMAIL_TOKEN_FILE')):
            token_file = os.getenv('GMAIL_TOKEN_FILE')
            credentials = google.oauth2.credentials.Credentials.from_authorized_user_file(token_file, scopes)
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(google.auth.transport.requests.Request())
            else:
                flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                    os.getenv('GMAIL_CREDENTIALS_FILE'),
                    scopes
                )
                credentials = flow.run_local_server(port=0)
            with open(os.getenv('GMAIL_TOKEN_FILE'), 'w') as token:
                token.write(credentials.to_json())
        try:
            LOGGER.info('Connected to the gmail API endpoint.')
            return googleapiclient.discovery.build('gmail', 'v1', credentials=credentials)

        except googleapiclient.errors.HttpError as e:
            LOGGER.error(f'Could not connect to the gmail service: {str(e)}')
            raise e

    def notify_status(self, gmail_service):
        # Compose the message to send.
        message = email.message.EmailMessage()
        message_content = f'Execution ID: {self.execution_id}\n'
        message_content += f'Configuration: {json.dumps(self.config, default=str)}\n'
        message_content += f'Client Class: {self.client_class}\n'
        message_content += f'Is Ended In Error: {self.execution_statistics.is_ended_in_error}\n'
        message_content += f'Total Executions: {self.execution_statistics.total_executions}\n'
        message_content += f'Total Running Time: {self.execution_statistics.total_running_time}s\n'
        message_content += f'Start Benchmark Time: {self.execution_statistics.start_time}\n'
        message_content += f'End Benchmark Time: {self.execution_statistics.end_time}\n'
        message_content += 'Queries Executed: ['
        for q in self.execution_statistics.queries_executed:
            message_content += f'\n\t-{q["queryFile"]} in {q["clientTime"]}s.'
        message_content += '\n]'
        message.set_content(message_content)
        LOGGER.info(message_content)

        # ...and send the email.
        message['To'] = os.getenv('GMAIL_NOTIFY_TO_EMAIL')
        message['From'] = os.getenv('GMAIL_NOTIFY_FROM_EMAIL')
        message['Subject'] = f'Execution "{self.execution_id}" has finished.'
        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        send_message = (gmail_service.users().messages().send(userId="me", body=create_message).execute())
        LOGGER.debug(f'Message (email) sent using ID {send_message["id"]}!')

    @staticmethod
    def sample_parameters(file_name: str, parameters: pandas.DataFrame) -> dict:
        data_frame_sample = {k: v.squeeze() for k, v in dict(parameters.sample()).items()}
        parameter_sample = dict()

        # First, check if we are working with BI parameters or interactive parameters.
        if any(':' in v for v in data_frame_sample.keys()):
            assert all((':' in v and v.count(':') == 1) for v in data_frame_sample.keys())
            for k, v in data_frame_sample.items():
                value_name, value_type = k.split(':')
                if value_type == 'INT' or value_type == 'ID' or value_type == 'STRING':
                    parameter_sample[value_name.lower()] = v
                elif value_type == 'DATE':
                    parameter_sample[value_name.lower()] = datetime.datetime.strptime(v, '%Y-%m-%d')
                elif value_type == 'DATETIME':
                    parameter_sample[value_name.lower()] = datetime.datetime.fromisoformat(v)
                elif value_type == 'STRING[]':
                    parameter_sample[value_name.lower()] = v.split(';')
                else:
                    raise TypeError(f'Unexpected type encountered: {value_type}')

        else:
            for k, v in data_frame_sample.items():
                if type(v) == numpy.int64:
                    parameter_sample[k.lower()] = int(v)
                elif type(v) == str:
                    parameter_sample[k.lower()] = v
                elif type(v) == pandas.Timestamp:
                    parameter_sample[k.lower()] = v.to_pydatetime()
                else:
                    raise TypeError(f'Unexpected type encountered: {str(type(v))}')

        # The driver includes a $limit parameter, but this does not show up in the parameter files. Add it here.
        if re.match(r'^(interactive-(4|6|10|11|personId))|(bi-17)', file_name):
            parameter_sample['limit'] = 10
        elif re.match(r'^(interactive-(2|3|5|7|8|9|12))|(bi-(3|16|18|20))', file_name):
            parameter_sample['limit'] = 20
        elif re.match(r'^bi-(2|4|5|6|7|8|9|10|13|14)', file_name):
            parameter_sample['limit'] = 100
        return parameter_sample

    @abc.abstractmethod
    def execute_query(self, query: str, output: multiprocessing.Queue) -> None:
        """ Execute a given query and return a dictionary of the results + any metrics. """
        pass

    @abc.abstractmethod
    def explain_query(self, query: str) -> typing.Dict:
        """ Do **not** execute the given query, but rather return a string of the query-plan that would be used. """
        pass

    @abc.abstractmethod
    def construct_query(self, raw_query: str, parameters: typing.Dict[str, typing.Any]) -> str:
        """ Parameterize the given query. """
        pass

    def __str__(self):
        return f'{{"executionId": {self.execution_id}, "clientClass": {self.client_class}}}'

    def __call__(self, *args, **kwargs):
        LOGGER.info(f'Starting benchmark! ({str(self)})')

        # Read in our parameters into memory.
        parameter_map = dict()
        for parameter_file in self.parameter_files:
            parameter_key = os.path.basename(parameter_file)
            if parameter_key.endswith('.parquet'):
                data_frame = pandas.read_parquet(parameter_file, engine='fastparquet')
                parameter_map[parameter_key] = data_frame
            elif parameter_key.endswith('.csv'):
                data_frame = pandas.read_csv(parameter_file, sep='|', header=0)
                parameter_map[parameter_key] = data_frame
            if parameter_map[parameter_key].empty:
                LOGGER.warning('Parameter files are empty for: ' + parameter_file + '!')

        # Read in the raw query files into memory.
        query_map = dict()
        for query_file in self.query_files:
            query_key = os.path.basename(query_file)
            with open(query_file, 'r') as query_fp:
                query_map[query_key] = query_fp.read()

        # Randomly generate a workload based on our given specification.
        random.seed(self.random_seed)
        workload = list()
        for query in self.specification:
            for _ in range(query.repeat):
                for parameter_file in query.parameter_files:
                    workload.append(ClientQuery(
                        parameter_file=parameter_file,
                        query_file=query.query_file,
                        timeout=query.timeout
                    ))
        random.shuffle(workload)
        blacklisted_queries = set()

        # Execute our restart command before starting the workload (to clear our cache).
        LOGGER.info(f'Restarting database using command: {self.restart_command}')
        self.call_subprocess(self.restart_command, is_log=self.is_debug)

        # Execute our workload.
        self.execution_statistics.start_time = datetime.datetime.now()
        t_execution_start = timeit.default_timer()
        for i, query in enumerate(workload):
            if query.query_file in blacklisted_queries:
                LOGGER.info(f'Skipping execution {i} for query file {query.query_file}.')
                continue
            else:
                LOGGER.info(f'Entering execution {i} using query file {query.query_file}.')

            # Parameterize our query.
            raw_query = query_map[query.query_file]
            parameters = self.sample_parameters(query.parameter_file, parameter_map[query.parameter_file])
            parameterized_query = self.construct_query(raw_query, parameters)
            LOGGER.info(f'Executing parameterized query:\n{parameterized_query}')

            # To handle transient errors, a query may potentially re-execute.
            is_error, client_time, process = False, 0, None
            for r in range(self.retries):
                execution_record_base = {
                    'queryFile': query.query_file,
                    'query': parameterized_query,
                    'parameters': parameters
                }

                # Execute our query.
                result_output = multiprocessing.Queue()
                process = multiprocessing.Process(
                    target=self.execute_query,
                    args=(parameterized_query, result_output,)
                )
                t_before = timeit.default_timer()
                process.start()
                process.join(query.timeout)
                client_time = timeit.default_timer() - t_before

                # Explain our query after executing it.
                query_plan = self.explain_query(parameterized_query)
                execution_record_base['queryPlan'] = query_plan

                # Kill our query if we need to.
                if process.is_alive():
                    LOGGER.info(f'Timeout of {query.timeout}s reached for query:\n{parameterized_query}')
                    self.log_execution({**execution_record_base, 'clientTime': client_time, 'status': 'timeout'})
                    process.kill()
                    process.join()
                    LOGGER.info(f'Issuing restart command and adding query {query.query_file} to the blacklist.')
                    self.call_subprocess(self.restart_command, is_log=self.is_debug)
                    blacklisted_queries.add(query.query_file)

                else:
                    result = result_output.get()
                    if result['status'] != 'success' and r < (self.retries - 1):
                        LOGGER.warning(f'Non success status returned: {result["status"]}. Restarting the query.')
                        time.sleep(5)
                        continue

                    elif result['status'] == 'transient':
                        LOGGER.warning(f'Transient exception status returned: {result["status"]}. Skipping the query.')
                        LOGGER.info(f'Issuing restart command and adding query {query.query_file} to the blacklist.')
                        self.call_subprocess(self.restart_command, is_log=self.is_debug)
                        blacklisted_queries.add(query.query_file)

                    elif result['status'] != 'success':
                        is_error = True
                        break

                    self.log_execution({**result, **execution_record_base, 'clientTime': client_time})

                # Gather query statistics.
                self.execution_statistics.end_time = datetime.datetime.now()
                self.execution_statistics.total_running_time = timeit.default_timer() - t_execution_start
                self.execution_statistics.total_executions += 1
                self.execution_statistics.queries_executed += [{
                    'queryFile': query.query_file,
                    'clientTime': client_time
                }]
                break

            if is_error:
                LOGGER.error('Exiting due to non-success status given by query execution.')
                self.execution_statistics.is_ended_in_error = True
                break

        # Flush our loggers.
        LOGGER.info('Flushing the logger.')
        [h.flush() for h in LOGGER.handlers]

        # Finally, notify us through email on the status of this execution.
        self.results_fp.close()
        if self.is_notify:
            LOGGER.info('Sending notification via email.')
            self.notify_status(self.create_gmail_service())
        LOGGER.info('Benchmark has finished executing!')
