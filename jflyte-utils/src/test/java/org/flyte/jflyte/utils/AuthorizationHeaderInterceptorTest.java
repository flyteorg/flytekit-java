/*
 * Copyright 2021-2023 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.jflyte.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import flyteidl.admin.Common;
import flyteidl.admin.ProjectOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.time.Instant;
import org.flyte.jflyte.api.Token;
import org.flyte.jflyte.api.TokenSource;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

class AuthorizationHeaderInterceptorTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  // Source:
  // https://github.com/grpc/grpc-java/blob/master/examples/src/test/java/io/grpc/examples/header/HeaderClientInterceptorTest.java
  private final ServerInterceptor mockServerInterceptor =
      mock(
          ServerInterceptor.class,
          delegatesTo(
              new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                  return next.startCall(call, headers);
                }
              }));

  @Test
  public void authHeaderDeliveredToServer() throws Exception {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    TestAdminService stubService = new TestAdminService();
    ServerServiceDefinition interceptedService =
        ServerInterceptors.intercept(stubService, mockServerInterceptor);

    Server build = GrpcUtils.buildServer(serverName, interceptedService);
    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(build.start());
    // Create a client channel and register for automatic graceful shutdown.
    ManagedChannel channel = grpcCleanup.register(GrpcUtils.buildChannel(serverName));

    String idToken = "awesome";
    String tokenType = "Bearer";

    Token token =
        Token.builder().tokenType(tokenType).accessToken(idToken).expiry(Instant.now()).build();
    TokenSource tokenSource = mock(TokenSource.class);
    when(tokenSource.token()).thenReturn(token);

    AdminServiceGrpc.AdminServiceBlockingStub blockingStub =
        AdminServiceGrpc.newBlockingStub(
            ClientInterceptors.intercept(channel, new AuthorizationHeaderInterceptor(tokenSource)));

    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);

    Common.ResourceListRequest listRequest =
        Common.ResourceListRequest.newBuilder().setLimit(100).build();
    blockingStub.listWorkflows(listRequest);

    String expected = tokenType + " " + idToken;

    verify(mockServerInterceptor)
        .interceptCall(
            ArgumentMatchers
                .<ServerCall<ProjectOuterClass.ProjectListRequest, ProjectOuterClass.Projects>>
                    any(),
            metadataCaptor.capture(),
            ArgumentMatchers
                .<ServerCallHandler<
                        ProjectOuterClass.ProjectListRequest, ProjectOuterClass.Projects>>
                    any());
    assertEquals(
        expected,
        metadataCaptor.getValue().get(AuthorizationHeaderInterceptor.AUTHENTICATION_HEADER_KEY));
    assertThat(stubService.listWorkflowsRequest, equalTo(listRequest));
  }
}
