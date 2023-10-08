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
package org.apache.asterix.graphix.test.benchmark.operational;

import java.util.Map;

import org.apache.asterix.graphix.test.benchmark.GraphixDB;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery10Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery11Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery12Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery1Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery2Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3a;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3b;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery4Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery5Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery6Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery7Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery8Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery9Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery1PersonProfileResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery2PersonPostsResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery3PersonFriendsResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery4MessageContentResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery5MessageCreatorResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery6MessageForumResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery7MessageReplies;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery7MessageRepliesResult;

public class GraphixOperationalDB extends GraphixDB {
    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {
        super.onInit(map, loggingService);

        // Register our short queries.
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, ShortQuery1.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, ShortQuery2.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, ShortQuery3.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, ShortQuery4.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, ShortQuery5.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, ShortQuery6.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, ShortQuery7.class);

        // Register our complex queries.
        registerOperationHandler(LdbcQuery1.class, ComplexQuery1.class);
        registerOperationHandler(LdbcQuery2.class, ComplexQuery2.class);
        registerOperationHandler(LdbcQuery3a.class, ComplexQuery3a.class);
        registerOperationHandler(LdbcQuery3b.class, ComplexQuery3b.class);
        registerOperationHandler(LdbcQuery4.class, ComplexQuery4.class);
        registerOperationHandler(LdbcQuery5.class, ComplexQuery5.class);
        registerOperationHandler(LdbcQuery6.class, ComplexQuery6.class);
        registerOperationHandler(LdbcQuery7.class, ComplexQuery7.class);
        registerOperationHandler(LdbcQuery8.class, ComplexQuery8.class);
        registerOperationHandler(LdbcQuery9.class, ComplexQuery9.class);
        registerOperationHandler(LdbcQuery10.class, ComplexQuery10.class);
        registerOperationHandler(LdbcQuery11.class, ComplexQuery11.class);
        registerOperationHandler(LdbcQuery12.class, ComplexQuery12.class);
        registerOperationHandler(LdbcQuery13a.class, ComplexQuery13a.class);
        registerOperationHandler(LdbcQuery13b.class, ComplexQuery13b.class);
        registerOperationHandler(LdbcQuery14a.class, ComplexQuery14a.class);
        registerOperationHandler(LdbcQuery14b.class, ComplexQuery14b.class);
    }

    public static class ShortQuery1
            extends SingleResultQuery<LdbcShortQuery1PersonProfile, LdbcShortQuery1PersonProfileResult> {
        @Override
        protected Class<LdbcShortQuery1PersonProfileResult> getResultType() {
            return LdbcShortQuery1PersonProfileResult.class;
        }
    }

    public static class ShortQuery2
            extends MultipleResultQuery<LdbcShortQuery2PersonPosts, LdbcShortQuery2PersonPostsResult> {
        @Override
        protected Class<LdbcShortQuery2PersonPostsResult> getResultType() {
            return LdbcShortQuery2PersonPostsResult.class;
        }
    }

    public static class ShortQuery3
            extends MultipleResultQuery<LdbcShortQuery3PersonFriends, LdbcShortQuery3PersonFriendsResult> {
        @Override
        protected Class<LdbcShortQuery3PersonFriendsResult> getResultType() {
            return LdbcShortQuery3PersonFriendsResult.class;
        }
    }

    public static class ShortQuery4
            extends SingleResultQuery<LdbcShortQuery4MessageContent, LdbcShortQuery4MessageContentResult> {
        @Override
        protected Class<LdbcShortQuery4MessageContentResult> getResultType() {
            return LdbcShortQuery4MessageContentResult.class;
        }
    }

    public static class ShortQuery5
            extends SingleResultQuery<LdbcShortQuery5MessageCreator, LdbcShortQuery5MessageCreatorResult> {
        @Override
        protected Class<LdbcShortQuery5MessageCreatorResult> getResultType() {
            return LdbcShortQuery5MessageCreatorResult.class;
        }
    }

    public static class ShortQuery6
            extends SingleResultQuery<LdbcShortQuery6MessageForum, LdbcShortQuery6MessageForumResult> {
        @Override
        protected Class<LdbcShortQuery6MessageForumResult> getResultType() {
            return LdbcShortQuery6MessageForumResult.class;
        }
    }

    public static class ShortQuery7
            extends MultipleResultQuery<LdbcShortQuery7MessageReplies, LdbcShortQuery7MessageRepliesResult> {
        @Override
        protected Class<LdbcShortQuery7MessageRepliesResult> getResultType() {
            return LdbcShortQuery7MessageRepliesResult.class;
        }
    }

    public static class ComplexQuery1 extends MultipleResultQuery<LdbcQuery1, LdbcQuery1Result> {
        @Override
        protected Class<LdbcQuery1Result> getResultType() {
            return LdbcQuery1Result.class;
        }
    }

    public static class ComplexQuery2 extends MultipleResultQuery<LdbcQuery2, LdbcQuery2Result> {
        @Override
        protected Class<LdbcQuery2Result> getResultType() {
            return LdbcQuery2Result.class;
        }
    }

    public static class ComplexQuery3a extends MultipleResultQuery<LdbcQuery3a, LdbcQuery3Result> {
        @Override
        protected Class<LdbcQuery3Result> getResultType() {
            return LdbcQuery3Result.class;
        }
    }

    public static class ComplexQuery3b extends MultipleResultQuery<LdbcQuery3b, LdbcQuery3Result> {
        @Override
        protected Class<LdbcQuery3Result> getResultType() {
            return LdbcQuery3Result.class;
        }
    }

    public static class ComplexQuery4 extends MultipleResultQuery<LdbcQuery4, LdbcQuery4Result> {
        @Override
        protected Class<LdbcQuery4Result> getResultType() {
            return LdbcQuery4Result.class;
        }
    }

    public static class ComplexQuery5 extends MultipleResultQuery<LdbcQuery5, LdbcQuery5Result> {
        @Override
        protected Class<LdbcQuery5Result> getResultType() {
            return LdbcQuery5Result.class;
        }
    }

    public static class ComplexQuery6 extends MultipleResultQuery<LdbcQuery6, LdbcQuery6Result> {
        @Override
        protected Class<LdbcQuery6Result> getResultType() {
            return LdbcQuery6Result.class;
        }
    }

    public static class ComplexQuery7 extends MultipleResultQuery<LdbcQuery7, LdbcQuery7Result> {
        @Override
        protected Class<LdbcQuery7Result> getResultType() {
            return LdbcQuery7Result.class;
        }
    }

    public static class ComplexQuery8 extends MultipleResultQuery<LdbcQuery8, LdbcQuery8Result> {
        @Override
        protected Class<LdbcQuery8Result> getResultType() {
            return LdbcQuery8Result.class;
        }
    }

    public static class ComplexQuery9 extends MultipleResultQuery<LdbcQuery9, LdbcQuery9Result> {
        @Override
        protected Class<LdbcQuery9Result> getResultType() {
            return LdbcQuery9Result.class;
        }
    }

    public static class ComplexQuery10 extends MultipleResultQuery<LdbcQuery10, LdbcQuery10Result> {
        @Override
        protected Class<LdbcQuery10Result> getResultType() {
            return LdbcQuery10Result.class;
        }
    }

    public static class ComplexQuery11 extends MultipleResultQuery<LdbcQuery11, LdbcQuery11Result> {
        @Override
        protected Class<LdbcQuery11Result> getResultType() {
            return LdbcQuery11Result.class;
        }
    }

    public static class ComplexQuery12 extends MultipleResultQuery<LdbcQuery12, LdbcQuery12Result> {
        @Override
        protected Class<LdbcQuery12Result> getResultType() {
            return LdbcQuery12Result.class;
        }
    }

    public static class ComplexQuery13a extends SingleResultQuery<LdbcQuery13a, LdbcQuery13Result> {
        @Override
        protected Class<LdbcQuery13Result> getResultType() {
            return LdbcQuery13Result.class;
        }
    }

    public static class ComplexQuery13b extends SingleResultQuery<LdbcQuery13b, LdbcQuery13Result> {
        @Override
        protected Class<LdbcQuery13Result> getResultType() {
            return LdbcQuery13Result.class;
        }
    }

    public static class ComplexQuery14a extends MultipleResultQuery<LdbcQuery14a, LdbcQuery14Result> {
        @Override
        protected Class<LdbcQuery14Result> getResultType() {
            return LdbcQuery14Result.class;
        }
    }

    public static class ComplexQuery14b extends MultipleResultQuery<LdbcQuery14b, LdbcQuery14Result> {
        @Override
        protected Class<LdbcQuery14Result> getResultType() {
            return LdbcQuery14Result.class;
        }
    }
}
