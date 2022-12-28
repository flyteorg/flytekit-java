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
public abstract class SdkTransform {
  public abstract SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs);

  public SdkTransform withInput(String name, String value) {
    return withInput(name, SdkBindingData.ofString(value));
  }

  public SdkTransform withInput(String name, long value) {
    return withInput(name, SdkBindingData.ofInteger(value));
  }

  public SdkTransform withInput(String name, Instant value) {
    return withInput(name, SdkBindingData.ofDatetime(value));
  }

  public SdkTransform withInput(String name, Duration value) {
    return withInput(name, SdkBindingData.ofDuration(value));
  }

  public SdkTransform withInput(String name, boolean value) {
    return withInput(name, SdkBindingData.ofBoolean(value));
  }

  public SdkTransform withInput(String name, double value) {
    return withInput(name, SdkBindingData.ofFloat(value));
  }

  public SdkTransform withInput(String name, SdkStruct value) {
    return withInput(name, SdkBindingData.ofStruct(value));
  }

  public SdkTransform withInput(String name, SdkBindingData value) {
    return SdkPartialTransform.of(this, singletonMap(name, value));
  }

  public SdkTransform withUpstreamNode(SdkNode node) {
    return SdkPartialTransform.of(this, singletonList(node.getNodeId()));
  }

  public SdkTransform withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().name(name).build();
    return SdkPartialTransform.of(this, metadata);
  }

  SdkTransform withNameOverrideIfNotSet(String name) {
    return withNameOverride(name);
  }

  public SdkTransform withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().timeout(timeout).build();
    return SdkPartialTransform.of(this, metadata);
  }

  public String getName() {
    return getClass().getName();
  }
}
