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
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.flyte.api.v1.Node.START_NODE_ID;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

public class SdkWorkflowBuilder {
  private final Map<String, SdkNode> allNodes;
  private final Map<String, Variable> inputs;
  private final Map<String, Variable> outputVariables;
  private final Map<String, SdkBindingData> outputBindings;

  SdkWorkflowBuilder() {
    this.allNodes = new HashMap<>();
    // Using LinkedHashMap to preserve declaration order
    this.inputs = new LinkedHashMap<>();
    this.outputVariables = new LinkedHashMap<>();
    this.outputBindings = new LinkedHashMap<>();
  }

  public SdkNode apply(String nodeId, SdkTransform transform) {
    return applyInternal(nodeId, transform, emptyList(), Collections.emptyMap());
  }

  public SdkNode apply(String nodeId, SdkTransform transform, Map<String, SdkBindingData> inputs) {
    return applyInternal(nodeId, transform, emptyList(), inputs);
  }

  SdkNode applyInternal(
      String nodeId,
      SdkTransform transform,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> inputs) {
    SdkNode sdkNode = transform.apply(this, nodeId, upstreamNodeIds, inputs);
    applyInternal(sdkNode);

    return sdkNode;
  }

  public static SdkBindingData literalOfInteger(long value) {
    return SdkBindingData.ofInteger(value);
  }

  public static SdkBindingData literalOfFloat(double value) {
    return SdkBindingData.ofFloat(value);
  }

  public static SdkBindingData literalOfString(String value) {
    return SdkBindingData.ofString(value);
  }

  public static SdkBindingData literalOfBoolean(boolean value) {
    return SdkBindingData.ofBoolean(value);
  }

  public static SdkBindingData literalOfDatetime(Instant value) {
    return SdkBindingData.ofDatetime(value);
  }

  public static SdkBindingData literalOfDuration(Duration value) {
    return SdkBindingData.ofDuration(value);
  }

  public SdkBinding mapOf(String name1, SdkBindingData value1) {
    return SdkBinding.builder(this).put(name1, value1).build();
  }

  public SdkBinding mapOf(
      String name1, SdkBindingData value1, String name2, SdkBindingData value2) {
    return SdkBinding.builder(this).put(name1, value1).put(name2, value2).build();
  }

  public SdkBinding mapOf(
      String name1,
      SdkBindingData value1,
      String name2,
      SdkBindingData value2,
      String name3,
      SdkBindingData value3) {
    return SdkBinding.builder(this)
        .put(name1, value1)
        .put(name2, value2)
        .put(name3, value3)
        .build();
  }

  public SdkBinding mapOf(
      String name1,
      SdkBindingData value1,
      String name2,
      SdkBindingData value2,
      String name3,
      SdkBindingData value3,
      String name4,
      SdkBindingData value4) {
    return SdkBinding.builder(this)
        .put(name1, value1)
        .put(name2, value2)
        .put(name3, value3)
        .put(name4, value4)
        .build();
  }

  public SdkBinding mapOf(
      String name1,
      SdkBindingData value1,
      String name2,
      SdkBindingData value2,
      String name3,
      SdkBindingData value3,
      String name4,
      SdkBindingData value4,
      String name5,
      SdkBindingData value5) {
    return SdkBinding.builder(this)
        .put(name1, value1)
        .put(name2, value2)
        .put(name3, value3)
        .put(name4, value4)
        .put(name5, value5)
        .build();
  }

  public SdkBinding tupleOf(SdkNode... nodes) {
    SdkBinding.Builder builder = SdkBinding.builder(this);

    for (SdkNode node : nodes) {
      builder.add(node);
    }

    return builder.build();
  }

  public SdkBindingData inputOfInteger(String name) {
    return inputOfInteger(name, "");
  }

  public SdkBindingData inputOfInteger(String name, String help) {
    return inputOf(name, SimpleType.INTEGER, help);
  }

  public SdkBindingData inputOfString(String name) {
    return inputOfString(name, "");
  }

  public SdkBindingData inputOfString(String name, String help) {
    return inputOf(name, SimpleType.STRING, help);
  }

  public SdkBindingData inputOfBoolean(String name) {
    return inputOfBoolean(name, "");
  }

  public SdkBindingData inputOfBoolean(String name, String help) {
    return inputOf(name, SimpleType.BOOLEAN, help);
  }

  public SdkBindingData inputOfDatetime(String name) {
    return inputOfDatetime(name, "");
  }

  public SdkBindingData inputOfDatetime(String name, String help) {
    return inputOf(name, SimpleType.DATETIME, help);
  }

  public SdkBindingData inputOfDuration(String name) {
    return inputOfDuration(name, "");
  }

  public SdkBindingData inputOfDuration(String name, String help) {
    return inputOf(name, SimpleType.DURATION, help);
  }

  public SdkBindingData inputOfFloat(String name) {
    return inputOfFloat(name, "");
  }

  public SdkBindingData inputOfFloat(String name, String help) {
    return inputOf(name, SimpleType.FLOAT, help);
  }

  private SdkBindingData inputOf(String name, SimpleType type, String help) {
    LiteralType literalType = LiteralTypes.ofSimpleType(type);
    Variable variable = Variable.builder().literalType(literalType).description(help).build();
    SdkBindingData bindingData = SdkBindingData.ofOutputReference(START_NODE_ID, name, literalType);
    inputs.put(name, variable);
    return bindingData;
  }

  private Map<String, Variable> getInputVariables() {
    return unmodifiableMap(new LinkedHashMap<>(inputs));
  }

  public void output(String name, SdkBindingData value) {
    output(name, value, "");
  }

  public void output(String name, SdkBindingData value, String help) {
    outputVariables.put(
        name, Variable.builder().literalType(value.type()).description(help).build());
    outputBindings.put(name, value);
  }

  private List<Binding> getOutputBindings() {
    return outputBindings.entrySet().stream()
        .map(entry -> getBinding(entry.getKey(), entry.getValue()))
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
  }

  private Map<String, Variable> getOutputVariables() {
    return unmodifiableMap(new LinkedHashMap<>(outputVariables));
  }

  private Binding getBinding(String var_, SdkBindingData bindingData) {
    return Binding.builder().var_(var_).binding(bindingData.idl()).build();
  }

  public void applyInternal(SdkNode node) {
    allNodes.put(node.getNodeId(), node);
  }

  private List<Node> nodesToIdl() {
    return allNodes.values().stream()
        .map(SdkNode::toIdl)
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
  }

  public WorkflowTemplate toIdlTemplate() {
    WorkflowMetadata metadata = WorkflowMetadata.builder().build();
    List<Node> nodes = nodesToIdl();

    return WorkflowTemplate.builder()
        .metadata(metadata)
        .interface_(
            TypedInterface.builder()
                .inputs(getInputVariables())
                .outputs(getOutputVariables())
                .build())
        .outputs(getOutputBindings())
        .nodes(nodes)
        .build();
  }
}
