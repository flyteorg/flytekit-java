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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SdkBinding {
  private final SdkWorkflowBuilder builder;
  private final Map<String, SdkBindingData> bindingData;
  private final List<String> upstreamNodeIds;

  SdkBinding(
      SdkWorkflowBuilder builder,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> bindingData) {
    this.builder = builder;
    this.upstreamNodeIds = upstreamNodeIds;
    this.bindingData = bindingData;
  }

  public SdkNode apply(String nodeId, SdkTransform transform) {
    return builder.applyInternal(nodeId, transform, upstreamNodeIds, bindingData);
  }

  public static Builder builder(SdkWorkflowBuilder workflowBuilder) {
    return new Builder(workflowBuilder);
  }

  public static class Builder {
    private final SdkWorkflowBuilder workflowBuilder;
    private final Map<String, SdkBindingData> inputs = new HashMap<>();
    private final List<String> upstreamNodeIds = new ArrayList<>();

    private Builder(SdkWorkflowBuilder workflowBuilder) {
      this.workflowBuilder = workflowBuilder;
    }

    public Builder put(String name, SdkBindingData data) {
      SdkBindingData previous = inputs.put(name, data);

      if (previous != null) {
        throw new IllegalArgumentException(String.format("Duplicate input: [%s]", name));
      }

      return this;
    }

    public Builder add(SdkNode node) {
      // explicitly specify upstreamNodeIds if we don't use any outputs to preserve the execution
      // order
      if (node.getOutputs().isEmpty()) {
        upstreamNodeIds.add(node.getNodeId());
      }

      Set<String> duplicateInputs =
          node.getOutputs().keySet().stream()
              .filter(inputs::containsKey)
              .collect(Collectors.toSet());

      if (!duplicateInputs.isEmpty()) {
        throw new IllegalArgumentException(String.format("Duplicate inputs: %s", duplicateInputs));
      }

      inputs.putAll(node.getOutputs());

      return this;
    }

    public SdkBinding build() {
      return new SdkBinding(
          workflowBuilder,
          Collections.unmodifiableList(new ArrayList<>(upstreamNodeIds)),
          Collections.unmodifiableMap(new HashMap<>(inputs)));
    }
  }
}
