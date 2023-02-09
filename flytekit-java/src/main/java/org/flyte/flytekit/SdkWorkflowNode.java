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

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.WorkflowNode;

/** Represent a {@link org.flyte.flytekit.SdkWorkflow} in a workflow DAG. */
public class SdkWorkflowNode<T> extends SdkNode<T> {
  private final String nodeId;
  private final List<String> upstreamNodeIds;
  private final SdkNodeMetadata metadata;
  private final WorkflowNode workflowNode;
  private final Map<String, SdkBindingData<?>> inputBindings;
  private final Map<String, SdkBindingData<?>> outputBindings;
  private final T outputs;

  SdkWorkflowNode(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      WorkflowNode workflowNode,
      Map<String, SdkBindingData<?>> inputBindings,
      Map<String, SdkBindingData<?>> outputBindings,
      T outputs) {
    super(builder);

    this.nodeId = nodeId;
    this.upstreamNodeIds = upstreamNodeIds;
    this.metadata = metadata;
    this.workflowNode = workflowNode;
    this.inputBindings = inputBindings;
    this.outputBindings = outputBindings;
    this.outputs = outputs;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, SdkBindingData<?>> getOutputBindings() {
    return outputBindings;
  }

  /** {@inheritDoc} */
  @Override
  public T getOutputs() {
    return outputs;
  }

  /** {@inheritDoc} */
  @Override
  public String getNodeId() {
    return nodeId;
  }

  /** {@inheritDoc} */
  @Override
  public Node toIdl() {
    // inputs in var order for predictability
    List<Binding> inputBindings =
        this.inputBindings.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(x -> toBinding(x.getKey(), x.getValue()))
            .collect(toUnmodifiableList());

    return Node.builder()
        .id(nodeId)
        .inputs(inputBindings)
        .workflowNode(workflowNode)
        .upstreamNodeIds(upstreamNodeIds)
        .metadata((metadata == null) ? null : metadata.toIdl())
        .build();
  }

  private static Binding toBinding(String var_, SdkBindingData<?> sdkBindingData) {
    return Binding.builder().var_(var_).binding(sdkBindingData.idl()).build();
  }
}
