/*
 * Copyright 2021 Flyte Authors
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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.flyte.jflyte.api.Token;
import org.flyte.jflyte.api.TokenSource;

/**
 * An interceptor to add authentication header. Ref:
 * https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/header/HeaderClientInterceptor.java
 */
class AuthorizationHeaderInterceptor implements ClientInterceptor {

  static final Metadata.Key<String> AUTHENTICATION_HEADER_KEY =
      Metadata.Key.of("flyte-authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final TokenSource tokenSource;

  AuthorizationHeaderInterceptor(TokenSource tokenSource) {
    this.tokenSource = tokenSource;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        Token token = tokenSource.token();
        headers.put(AUTHENTICATION_HEADER_KEY, token.tokenType() + " " + token.accessToken());

        super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {}, headers);
      }
    };
  }
}
