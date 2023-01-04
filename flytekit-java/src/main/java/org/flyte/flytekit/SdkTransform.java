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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Implementations of {@code SdkTransform} transform {@link SdkNode} into a new one. */
public abstract class SdkTransform<T> {

  public abstract SdkType<T> getOutputType();

  public abstract SdkNode<T> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs);

  public SdkTransform<T> withInput(String name, String value) {
    return withInput(name, SdkBindingData.ofString(value));
  }

  public SdkTransform<T> withInput(String name, long value) {
    return withInput(name, SdkBindingData.ofInteger(value));
  }

  public SdkTransform<T> withInput(String name, Instant value) {
    return withInput(name, SdkBindingData.ofDatetime(value));
  }

  public SdkTransform<T> withInput(String name, Duration value) {
    return withInput(name, SdkBindingData.ofDuration(value));
  }

  public SdkTransform<T> withInput(String name, boolean value) {
    return withInput(name, SdkBindingData.ofBoolean(value));
  }

  public SdkTransform<T> withInput(String name, double value) {
    return withInput(name, SdkBindingData.ofFloat(value));
  }

  // TODO SdkStruct is not strongly typed. We need to decide if we make this a SdkTransform<?>, a
  // SdkTransform<SdkStruct>
  //     or even better SdkTransform<T> withInput(String name, SdkType<T> type, T value)
  public SdkTransform<?> withInput(String name, SdkStruct value) {
    return null;
    // return withInput(name, SdkBindingData.ofStruct(value));
  }

  public SdkTransform<T> withInput(String name, SdkBindingData<?> value) {
    return SdkPartialTransform.of(this, singletonMap(name, value));
  }

  public <K> SdkTransform<T> withUpstreamNode(SdkNode<K> node) {
    return SdkPartialTransform.of(this, singletonList(node.getNodeId()));
  }

  public SdkTransform<T> withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().name(name).build();
    return SdkPartialTransform.of(this, metadata);
  }

  public SdkTransform<T> withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().timeout(timeout).build();
    return SdkPartialTransform.of(this, metadata);
  }
}
