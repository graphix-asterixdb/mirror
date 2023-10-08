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
package org.apache.asterix.graphix.test.benchmark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery7MessageReplies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class GraphixConnectionState extends DbConnectionState {
    private final CloseableHttpClient httpClient;
    private final String asterixDBEndpoint;
    private final ObjectMapper objectMapper;
    private final Map<Integer, Pair<String, String>> queryMap;
    private final String queryTimeout;

    // The queries are given below.
    private static final Map<Function<String, Boolean>, Integer> fileMap;

    static {
        fileMap = new HashMap<>();

        // The short queries are given below.
        BiConsumer<String, Integer> fileMapUpdater = (s, k) -> fileMap.put(i -> Pattern.matches(s, i), k);
        fileMapUpdater.accept("short-1.sqlpp$", LdbcShortQuery1PersonProfile.TYPE);
        fileMapUpdater.accept("short-2.sqlpp$", LdbcShortQuery2PersonPosts.TYPE);
        fileMapUpdater.accept("short-3.sqlpp$", LdbcShortQuery3PersonFriends.TYPE);
        fileMapUpdater.accept("short-4.sqlpp$", LdbcShortQuery4MessageContent.TYPE);
        fileMapUpdater.accept("short-5.sqlpp$", LdbcShortQuery5MessageCreator.TYPE);
        fileMapUpdater.accept("short-6.sqlpp$", LdbcShortQuery6MessageForum.TYPE);
        fileMapUpdater.accept("short-7.sqlpp$", LdbcShortQuery7MessageReplies.TYPE);

        // The complex queries are given below.
        fileMapUpdater.accept("complex-1.sqlpp$", LdbcQuery1.TYPE);
        fileMapUpdater.accept("complex-2.sqlpp$", LdbcQuery2.TYPE);
        fileMapUpdater.accept("complex-3.sqlpp$", LdbcQuery3a.TYPE);
        fileMapUpdater.accept("complex-3.sqlpp$", LdbcQuery3b.TYPE);
        fileMapUpdater.accept("complex-4.sqlpp$", LdbcQuery4.TYPE);
        fileMapUpdater.accept("complex-5.sqlpp$", LdbcQuery5.TYPE);
        fileMapUpdater.accept("complex-6.sqlpp$", LdbcQuery6.TYPE);
        fileMapUpdater.accept("complex-7.sqlpp$", LdbcQuery7.TYPE);
        fileMapUpdater.accept("complex-8.sqlpp$", LdbcQuery8.TYPE);
        fileMapUpdater.accept("complex-9.sqlpp$", LdbcQuery9.TYPE);
        fileMapUpdater.accept("complex-10.sqlpp$", LdbcQuery10.TYPE);
        fileMapUpdater.accept("complex-11.sqlpp$", LdbcQuery11.TYPE);
        fileMapUpdater.accept("complex-12.sqlpp$", LdbcQuery12.TYPE);
        fileMapUpdater.accept("complex-13.sqlpp$", LdbcQuery13a.TYPE);
        fileMapUpdater.accept("complex-13.sqlpp$", LdbcQuery13b.TYPE);
        fileMapUpdater.accept("complex-14.sqlpp$", LdbcQuery14a.TYPE);
        fileMapUpdater.accept("complex-14.sqlpp$", LdbcQuery14b.TYPE);
    }

    public GraphixConnectionState(Map<String, String> parameterMap) throws DbException {
        String ccAddress = parameterMap.get("org.apache.asterix.ccAddress");
        String ccPort = parameterMap.get("org.apache.asterix.ccPort");
        this.asterixDBEndpoint = String.format("http://%s:%s/query/service", ccAddress, ccPort);
        this.httpClient = HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
        this.queryMap = new HashMap<>();

        // We want to make sure that our queryDir property points to the correct place.
        File queryDir = new File(parameterMap.get("org.apache.asterix.graphix.queryDir"));
        if (!Files.isDirectory(queryDir.toPath())) {
            throw new DbException("Invalid argument: queryDir is not a path!");
        }
        try (Stream<Path> entries = Files.list(queryDir.toPath())) {
            if (!entries.findFirst().isPresent()) {
                throw new DbException("No files found in queryDir! Are you sure this is the right path?");
            }
            Iterator<File> fileIterator = FileUtils.iterateFiles(queryDir, new SuffixFileFilter(".sqlpp"), null);
            while (fileIterator.hasNext()) {
                // Read in the queries according to the regex map above.
                File queryFile = fileIterator.next();
                Integer queryID = fileMap.entrySet().stream().filter(p -> p.getKey().apply(queryFile.getName()))
                        .findFirst().orElseThrow(() -> new IllegalArgumentException("Query file cannot be mapped!"))
                        .getValue();

                // Read the query file in.
                String queryString = IOUtils.toString(Files.newInputStream(queryFile.toPath()), StandardCharsets.UTF_8);
                String queryName = queryFile.getName().replace(".sqlpp", "");
                queryMap.put(queryID, Pair.of(queryName, queryString));
            }
        } catch (IOException e) {
            throw new DbException(e);
        }

        // Set our timeout.
        queryTimeout = parameterMap.get("org.apache.asterix.timeout");
    }

    public String getQuery(int queryID) {
        return queryMap.get(queryID).getRight();
    }

    public <T> List<T> executeStatement(String statement, Class<T> resultType) throws DbException {
        // We are not logging our queries, we are executing them.
        List<BasicNameValuePair> postParameters = new ArrayList<>();
        postParameters.add(new BasicNameValuePair("statement", statement));
        postParameters.add(new BasicNameValuePair("timeout", queryTimeout));
        try {
            HttpPost httpPost = new HttpPost(asterixDBEndpoint);
            httpPost.setEntity(new UrlEncodedFormEntity(postParameters, "UTF-8"));
            HttpResponse response = httpClient.execute(httpPost);

            // Hopefully we get an entity back... :-)
            if (response.getEntity() == null) {
                throw new DbException(new IllegalStateException("No response from AsterixDB!"));
            }

            // ...and a "success" status.
            InputStream responseContent = response.getEntity().getContent();
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            JavaType responseType = typeFactory.constructParametricType(AsterixDBResponse.class, resultType);
            AsterixDBResponse<T> asterixDBResponse = objectMapper.readValue(responseContent, responseType);
            if (!asterixDBResponse.status.equalsIgnoreCase("success")) {
                throw new DbException(new IllegalStateException("Non-success result returned from AsterixDB!"));
            }

            // Return our results if we have any.
            return asterixDBResponse.results;

        } catch (IOException e) {
            throw new DbException(e);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class AsterixDBResponse<T> {
        public String status;
        public List<T> results;
    }
}
