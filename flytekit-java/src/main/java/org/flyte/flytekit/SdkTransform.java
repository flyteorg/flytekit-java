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
package org.flyte.flytekit;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

/** Implementations of {@code SdkTransform} transform {@link SdkNode} into a new one. */
public abstract class SdkTransform<InputT, OutputT> {

  public abstract SdkType<InputT> getInputType();

  public abstract SdkType<OutputT> getOutputType();

  public abstract SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      @Nullable InputT inputs);

  public SdkTransform<InputT, OutputT> withUpstreamNode(SdkNode<?> node) {
    return SdkPartialTransform.of(this, List.of(node.getNodeId()));
  }

  public SdkTransform<InputT, OutputT> withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().name(name).build();
    return SdkPartialTransform.of(this, metadata);
  }

  SdkTransform<InputT, OutputT> withNameOverrideIfNotSet(String name) {
    return withNameOverride(name);
  }

  public SdkTransform<InputT, OutputT> withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().timeout(timeout).build();
    return SdkPartialTransform.of(this, metadata);
  }

  public String getName() {
    return getClass().getName();
  }
}
