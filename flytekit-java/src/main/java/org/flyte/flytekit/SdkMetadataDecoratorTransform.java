/*
 * Copyright 2020-2023 Flyte Authors.
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

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

/** Decorator for {@link SdkTransform} for holding metadata. */
class SdkMetadataDecoratorTransform<InputT, OutputT> extends SdkTransform<InputT, OutputT> {
  private final SdkTransform<InputT, OutputT> transform;
  private final List<String> extraUpstreamNodeIds;
  @Nullable private final SdkNodeMetadata metadata;

  private SdkMetadataDecoratorTransform(
      SdkTransform<InputT, OutputT> transform,
      List<String> extraUpstreamNodeIds,
      @Nullable SdkNodeMetadata metadata) {
    this.transform = transform;
    this.extraUpstreamNodeIds = List.copyOf(extraUpstreamNodeIds);
    this.metadata = metadata;
  }

  static <InputT, OutputT> SdkTransform<InputT, OutputT> of(
      SdkTransform<InputT, OutputT> transform, List<String> extraUpstreamNodeIds) {
    return new SdkMetadataDecoratorTransform<>(transform, extraUpstreamNodeIds, /*metadata=*/ null);
  }

  static <InputT, OutputT> SdkTransform<InputT, OutputT> of(
      SdkTransform<InputT, OutputT> transform, SdkNodeMetadata metadata) {
    return new SdkMetadataDecoratorTransform<>(transform, List.of(), metadata);
  }

  @Override
  public SdkTransform<InputT, OutputT> withUpstreamNode(SdkNode<?> node) {
    if (extraUpstreamNodeIds.contains(node.getNodeId())) {
      throw new IllegalArgumentException(
          String.format("Duplicate upstream node id [%s]", node.getNodeId()));
    }

    List<String> newExtraUpstreamNodeIds = new ArrayList<>(extraUpstreamNodeIds);
    newExtraUpstreamNodeIds.add(node.getNodeId());

    return new SdkMetadataDecoratorTransform<>(
        transform, unmodifiableList(newExtraUpstreamNodeIds), metadata);
  }

  @Override
  public SdkTransform<InputT, OutputT> withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata newMetadata = SdkNodeMetadata.builder().name(name).build();
    checkForDuplicateMetadata(metadata, newMetadata, SdkNodeMetadata::name, "name");
    SdkNodeMetadata mergedMetadata = mergeMetadata(metadata, newMetadata);

    return new SdkMetadataDecoratorTransform<>(transform, extraUpstreamNodeIds, mergedMetadata);
  }

  @Override
  SdkTransform<InputT, OutputT> withNameOverrideIfNotSet(String name) {
    if (metadata != null && metadata.name() != null) {
      return this;
    }
    return withNameOverride(name);
  }

  @Override
  public SdkTransform<InputT, OutputT> withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata newMetadata = SdkNodeMetadata.builder().timeout(timeout).build();
    checkForDuplicateMetadata(metadata, newMetadata, SdkNodeMetadata::timeout, "timeout");
    SdkNodeMetadata mergedMetadata = mergeMetadata(metadata, newMetadata);

    return new SdkMetadataDecoratorTransform<>(transform, extraUpstreamNodeIds, mergedMetadata);
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return transform.getOutputType();
  }

  @Override
  public SdkType<InputT> getInputType() {
    return transform.getInputType();
  }

  @Override
  public String getName() {
    return transform.getName();
  }

  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {

    List<String> duplicates = new ArrayList<>(upstreamNodeIds);
    duplicates.retainAll(extraUpstreamNodeIds);

    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Duplicate upstream node ids %s", duplicates));
    }

    List<String> allUpstreamNodeIds = new ArrayList<>(upstreamNodeIds);
    allUpstreamNodeIds.addAll(extraUpstreamNodeIds);

    checkForDuplicateMetadata(this.metadata, metadata, SdkNodeMetadata::name, "name");
    checkForDuplicateMetadata(this.metadata, metadata, SdkNodeMetadata::timeout, "timeout");
    SdkNodeMetadata mergedMetadata = mergeMetadata(this.metadata, metadata);

    return transform.apply(
        builder, nodeId, unmodifiableList(allUpstreamNodeIds), mergedMetadata, inputs);
  }

  private static void checkForDuplicateMetadata(
      @Nullable SdkNodeMetadata m1,
      @Nullable SdkNodeMetadata m2,
      Function<SdkNodeMetadata, ?> function,
      String fieldName) {
    if (m1 != null && m2 != null && function.apply(m1) != null && function.apply(m2) != null) {
      throw new IllegalArgumentException(
          String.format("Duplicate values for metadata: %s", fieldName));
    }
  }

  private SdkNodeMetadata mergeMetadata(
      @Nullable SdkNodeMetadata m1, @Nullable SdkNodeMetadata m2) {
    if (m1 == null) {
      return m2;
    } else if (m2 == null) {
      return m1;
    }

    SdkNodeMetadata.Builder builder = m1.toBuilder();
    if (m2.name() != null) {
      builder.name(m2.name());
    }
    if (m2.timeout() != null) {
      builder.timeout(m2.timeout());
    }
    return builder.build();
  }
}
