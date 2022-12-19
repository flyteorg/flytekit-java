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

import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;
import static org.flyte.flytekit.MoreCollectors.toUnmodifiableMap;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.Variable;

/** Represent a {@link org.flyte.flytekit.SdkRunnableTask} in a workflow DAG. */
public class SdkTaskNode<T extends NamedOutput> extends SdkNode<T> {
  private final String nodeId;
  private final PartialTaskIdentifier taskId;
  private final List<String> upstreamNodeIds;
  @Nullable private final SdkNodeMetadata metadata;
  private final Map<String, SdkBindingData> inputs;
  private final Map<String, Variable> outputs;

  SdkTaskNode(
      SdkWorkflowBuilder builder,
      String nodeId,
      PartialTaskIdentifier taskId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs,
      Map<String, Variable> outputs,
      Class<T> namedOutputClass) {
    super(builder, namedOutputClass);

    this.nodeId = nodeId;
    this.taskId = taskId;
    this.upstreamNodeIds = upstreamNodeIds;
    this.metadata = metadata;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public Map<String, SdkBindingData> getOutputs() {
    return outputs.entrySet().stream()
        .collect(
            toUnmodifiableMap(
                Map.Entry::getKey,
                entry ->
                    SdkBindingData.ofOutputReference(
                        nodeId, entry.getKey(), entry.getValue().literalType())));
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public Node toIdl() {
    TaskNode taskNode = TaskNode.builder().referenceId(taskId).build();

    List<Binding> bindings =
        inputs.entrySet().stream()
            .map(x -> toBinding(x.getKey(), x.getValue()))
            .collect(toUnmodifiableList());

    return Node.builder()
        .id(nodeId)
        .upstreamNodeIds(upstreamNodeIds)
        .metadata((metadata == null) ? null : metadata.toIdl())
        .taskNode(taskNode)
        .inputs(bindings)
        .build();
  }

  private static Binding toBinding(String var_, SdkBindingData sdkBindingData) {
    return Binding.builder().var_(var_).binding(sdkBindingData.idl()).build();
  }
}
