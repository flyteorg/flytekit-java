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
package org.flyte.jflyte;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class GrpcClientRpcOriginMetadataInterceptor implements ClientInterceptor {

  private final String rpcOriginValue;

  private GrpcClientRpcOriginMetadataInterceptor(String rpcOriginValue) {
    this.rpcOriginValue = rpcOriginValue;
  }

  public static GrpcClientRpcOriginMetadataInterceptor create(String rpcOriginValue) {
    return new GrpcClientRpcOriginMetadataInterceptor(rpcOriginValue);
  }

  static final Metadata.Key<String> RPC_ORIGIN_HEADER_KEY =
      Metadata.Key.of("rpc-origin", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        Metadata metadata = new Metadata();
        metadata.put(RPC_ORIGIN_HEADER_KEY, rpcOriginValue);
        headers.merge(metadata);
        super.start(responseListener, headers);
      }
    };
  }
}
