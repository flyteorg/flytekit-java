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

import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;

public class SdkTaskNode extends SdkNode {
  private final String nodeId;
  private final SdkRunnableTask<?, ?> task;
  private final Map<String, SdkBindingData> inputs;

  SdkTaskNode(
      SdkWorkflowBuilder builder,
      String nodeId,
      SdkRunnableTask<?, ?> task,
      Map<String, SdkBindingData> inputs) {
    super(builder);

    this.nodeId = nodeId;
    this.task = task;
    this.inputs = inputs;
  }

  @Override
  public Map<String, SdkBindingData> getOutputs() {
    return task.getOutputType().getVariableMap().entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                Map.Entry::getKey,
                entry -> SdkBindingData.ofOutputReference(nodeId, entry.getKey())));
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public Node toIdl(SdkConfig config) {
    TaskNode taskNode =
        TaskNode.create(
            TaskIdentifier.create(
                /* domain= */ config.domain(),
                /* project= */ config.project(),
                /* name= */ task.getName(),
                /* version= */ config.version()));

    List<Binding> bindings =
        inputs.entrySet().stream()
            .map(x -> Binding.create(x.getKey(), x.getValue().toIdl()))
            .collect(ImmutableList.toImmutableList());

    return Node.builder().id(nodeId).taskNode(taskNode).inputs(bindings).build();
  }
}
