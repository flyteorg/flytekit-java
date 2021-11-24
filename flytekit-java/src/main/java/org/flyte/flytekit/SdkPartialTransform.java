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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

/** {@link SdkTransform} with partially specified inputs. */
class SdkPartialTransform extends SdkTransform {
  private final SdkTransform transform;
  private final Map<String, SdkBindingData> fixedInputs;
  private final List<String> extraUpstreamNodeIds;
  @Nullable private final SdkNodeMetadata metadata;

  private SdkPartialTransform(
      SdkTransform transform,
      Map<String, SdkBindingData> fixedInputs,
      List<String> extraUpstreamNodeIds,
      @Nullable SdkNodeMetadata metadata) {
    this.transform = transform;
    this.fixedInputs = unmodifiableMap(new HashMap<>(fixedInputs));
    this.extraUpstreamNodeIds = unmodifiableList(new ArrayList<>(extraUpstreamNodeIds));
    this.metadata = metadata;
  }

  static SdkTransform of(SdkTransform transform, Map<String, SdkBindingData> fixedInputs) {
    return new SdkPartialTransform(transform, fixedInputs, emptyList(), /*metadata=*/ null);
  }

  static SdkTransform of(SdkTransform transform, List<String> extraUpstreamNodeIds) {
    return new SdkPartialTransform(transform, emptyMap(), extraUpstreamNodeIds, /*metadata=*/ null);
  }

  static SdkTransform of(SdkTransform transform, SdkNodeMetadata metadata) {
    return new SdkPartialTransform(transform, emptyMap(), emptyList(), metadata);
  }

  @Override
  public SdkTransform withInput(String name, SdkBindingData value) {
    // isn't necessary to override, but this reduces nesting and gives better error messages

    SdkBindingData existing = fixedInputs.get(name);
    if (existing != null) {
      String message =
          String.format("Duplicate values for input [%s]: [%s], [%s]", name, existing, value);
      throw new IllegalArgumentException(message);
    }

    Map<String, SdkBindingData> newFixedInputs = new HashMap<>(fixedInputs);
    newFixedInputs.put(name, value);

    return new SdkPartialTransform(
        transform, unmodifiableMap(newFixedInputs), extraUpstreamNodeIds, metadata);
  }

  @Override
  public SdkTransform withUpstreamNode(SdkNode node) {
    if (extraUpstreamNodeIds.contains(node.getNodeId())) {
      throw new IllegalArgumentException(
          String.format("Duplicate upstream node id [%s]", node.getNodeId()));
    }

    List<String> newExtraUpstreamNodeIds = new ArrayList<>(extraUpstreamNodeIds);
    newExtraUpstreamNodeIds.add(node.getNodeId());

    return new SdkPartialTransform(
        transform, fixedInputs, unmodifiableList(newExtraUpstreamNodeIds), metadata);
  }

  @Override
  public SdkTransform withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata newMetadata = SdkNodeMetadata.builder().name(name).build();
    checkForDuplicateMetadata(metadata, newMetadata, SdkNodeMetadata::name, "name");
    SdkNodeMetadata mergedMetadata = mergeMetadata(metadata, newMetadata);

    return new SdkPartialTransform(transform, fixedInputs, extraUpstreamNodeIds, mergedMetadata);
  }

  @Override
  public SdkTransform withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata newMetadata = SdkNodeMetadata.builder().timeout(timeout).build();
    checkForDuplicateMetadata(metadata, newMetadata, SdkNodeMetadata::timeout, "timeout");
    SdkNodeMetadata mergedMetadata = mergeMetadata(metadata, newMetadata);

    return new SdkPartialTransform(transform, fixedInputs, extraUpstreamNodeIds, mergedMetadata);
  }

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {
    Map<String, SdkBindingData> allInputs = new HashMap<>(fixedInputs);

    inputs.forEach(
        (k, v) ->
            allInputs.merge(
                k,
                v,
                (v1, v2) -> {
                  String message =
                      String.format("Duplicate values for input [%s]: [%s], [%s]", k, v1, v2);
                  throw new IllegalArgumentException(message);
                }));

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
        builder,
        nodeId,
        unmodifiableList(allUpstreamNodeIds),
        mergedMetadata,
        unmodifiableMap(allInputs));
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
