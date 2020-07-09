/*
 * Copyright 2020 Spotify AB.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link SdkTransform} with partially specified inputs. */
class SdkPartialTransform extends SdkTransform {
  private final SdkTransform transform;
  private final Map<String, SdkBindingData> fixedInputs;
  private final List<String> extraUpstreamNodeIds;

  private SdkPartialTransform(
      SdkTransform transform,
      Map<String, SdkBindingData> fixedInputs,
      List<String> extraUpstreamNodeIds) {
    this.transform = transform;
    this.fixedInputs = unmodifiableMap(new HashMap<>(fixedInputs));
    this.extraUpstreamNodeIds = unmodifiableList(new ArrayList<>(extraUpstreamNodeIds));
  }

  static SdkTransform of(SdkTransform transform, Map<String, SdkBindingData> fixedInputs) {
    return new SdkPartialTransform(transform, fixedInputs, emptyList());
  }

  static SdkTransform of(SdkTransform transform, List<String> extraUpstreamNodeIds) {
    return new SdkPartialTransform(transform, emptyMap(), extraUpstreamNodeIds);
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
        transform, unmodifiableMap(newFixedInputs), extraUpstreamNodeIds);
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
        transform, fixedInputs, unmodifiableList(newExtraUpstreamNodeIds));
  }

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> inputs) {
    Map<String, SdkBindingData> allInputs = new HashMap<>();

    fixedInputs.forEach(allInputs::put);

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

    return transform.apply(
        builder, nodeId, unmodifiableList(allUpstreamNodeIds), unmodifiableMap(allInputs));
  }
}
