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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

class WorkflowTemplateIdl {

  public static WorkflowTemplate ofBuilder(
      SdkWorkflowBuilder builder,
      Map<String, SdkBindingData<?>> inputs,
      Map<String, SdkBindingData<?>> outputs) {
    WorkflowMetadata metadata = WorkflowMetadata.builder().build();

    List<Node> nodes =
        builder.getNodes().values().stream().map(SdkNode::toIdl).collect(toUnmodifiableList());

    List<Binding> outputBindings = getOutputBindings(outputs);

    return WorkflowTemplate.builder()
        .metadata(metadata)
        .interface_(
            TypedInterface.builder()
                .inputs(getInputVariableMap(inputs))
                .outputs(getOutputVariableMap(outputs))
                .build())
        .outputs(outputBindings)
        .nodes(nodes)
        .build();
  }

  static List<Binding> getOutputBindings(Map<String, SdkBindingData<?>> outputs) {
    return outputs.entrySet().stream()
        .map(entry -> getBinding(entry.getKey(), entry.getValue()))
        .collect(toUnmodifiableList());
  }

  static Map<String, Variable> getInputVariableMap(Map<String, SdkBindingData<?>> inputs) {
    return inputs.entrySet().stream()
        .map(
            entry -> {
              Variable variable =
                  Variable.builder()
                      .literalType(entry.getValue().type())
                      .description("") // TODO description not supported currently
                      .build();

              return new SimpleImmutableEntry<>(entry.getKey(), variable);
            })
        .collect(toUnmodifiableMap());
  }

  static Map<String, Variable> getOutputVariableMap(Map<String, SdkBindingData<?>> outputs) {
    return outputs.entrySet().stream()
        .map(
            entry -> {
              Variable variable =
                  Variable.builder()
                      .literalType(entry.getValue().type())
                      .description("") // TODO description not supported currently
                      .build();

              return new SimpleImmutableEntry<>(entry.getKey(), variable);
            })
        .collect(toUnmodifiableMap());
  }

  private static Binding getBinding(String var_, SdkBindingData<?> bindingData) {
    return Binding.builder().var_(var_).binding(bindingData.idl()).build();
  }
}
