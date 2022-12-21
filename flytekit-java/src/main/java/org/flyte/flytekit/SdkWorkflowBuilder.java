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
import static java.util.Collections.unmodifiableMap;
import static org.flyte.api.v1.Node.START_NODE_ID;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.WorkflowTemplate;

public class SdkWorkflowBuilder {
  private final Map<String, SdkNode<?>> nodes;
  private final Map<String, SdkBindingData> inputs;
  private final Map<String, SdkBindingData> outputs;
  private final Map<String, String> inputDescriptions;
  private final Map<String, String> outputDescriptions;

  public SdkWorkflowBuilder() {
    // Using LinkedHashMap to preserve declaration order
    this.nodes = new LinkedHashMap<>();
    this.inputs = new LinkedHashMap<>();
    this.outputs = new LinkedHashMap<>();

    this.inputDescriptions = new HashMap<>();
    this.outputDescriptions = new HashMap<>();
  }

  public <T extends NamedOutput> SdkNode<T> apply(String nodeId, SdkTransform<T> transform) {
    return apply(nodeId, transform, emptyMap());
  }

  public <T extends NamedOutput> SdkNode<T> apply(
      String nodeId, SdkTransform<T> transform, Class<T> namedOutputClass) {
    return apply(nodeId, transform, emptyMap(), namedOutputClass);
  }

  public <T extends NamedOutput> SdkNode<T> apply(
      String nodeId,
      SdkTransform<T> transform,
      Map<String, SdkBindingData> inputs,
      Class<T> namedOutputClass) {
    return applyInternal(
        nodeId, transform, emptyList(), /*metadata=*/ null, inputs, namedOutputClass);
  }

  public <T extends NamedOutput> SdkNode<T> apply(
      String nodeId, SdkTransform<T> transform, Map<String, SdkBindingData> inputs) {
    SdkBindingData.ofString("")
    return applyInternal(nodeId, transform, emptyList(), /*metadata=*/ null, inputs, null);
  }

  protected <T extends NamedOutput> SdkNode<T> applyInternal(
      String nodeId,
      SdkTransform<T> transform,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs,
      Class<T> namedOutputClass) {

    if (nodes.containsKey(nodeId)) {
      CompilerError error =
          CompilerError.create(
              CompilerError.Kind.DUPLICATE_NODE_ID,
              nodeId,
              "Trying to insert two nodes with the same id.");

      throw new CompilerException(error);
    }

    SdkNode<T> sdkNode = transform.apply(this, nodeId, upstreamNodeIds, metadata, inputs);
    nodes.put(sdkNode.getNodeId(), sdkNode);

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

  public SdkBindingData inputOfInteger(String name) {
    return inputOfInteger(name, "");
  }

  public SdkBindingData inputOfInteger(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.INTEGER), help);
  }

  public SdkBindingData inputOfString(String name) {
    return inputOfString(name, "");
  }

  public SdkBindingData inputOfString(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.STRING), help);
  }

  public SdkBindingData inputOfBoolean(String name) {
    return inputOfBoolean(name, "");
  }

  public SdkBindingData inputOfBoolean(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.BOOLEAN), help);
  }

  public SdkBindingData inputOfDatetime(String name) {
    return inputOfDatetime(name, "");
  }

  public SdkBindingData inputOfDatetime(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.DATETIME), help);
  }

  public SdkBindingData inputOfDuration(String name) {
    return inputOfDuration(name, "");
  }

  public SdkBindingData inputOfDuration(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.DURATION), help);
  }

  public SdkBindingData inputOfFloat(String name) {
    return inputOfFloat(name, "");
  }

  public SdkBindingData inputOfFloat(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.FLOAT), help);
  }

  public SdkBindingData inputOfStruct(String name) {
    return inputOfStruct(name, "");
  }

  public SdkBindingData inputOfStruct(String name, String help) {
    return inputOf(name, LiteralType.ofSimpleType(SimpleType.STRUCT), help);
  }

  public SdkBindingData inputOf(String name, LiteralType literalType, String help) {
    SdkBindingData bindingData = SdkBindingData.ofOutputReference(START_NODE_ID, name, literalType);

    inputDescriptions.put(name, help);
    inputs.put(name, bindingData);

    return bindingData;
  }

  public Map<String, SdkNode<?>> getNodes() {
    return unmodifiableMap(new LinkedHashMap<>(nodes));
  }

  public Map<String, SdkBindingData> getInputs() {
    return unmodifiableMap(new LinkedHashMap<>(inputs));
  }

  public String getInputDescription(String name) {
    return inputDescriptions.getOrDefault(name, "");
  }

  public Map<String, SdkBindingData> getOutputs() {
    return unmodifiableMap(new LinkedHashMap<>(outputs));
  }

  public String getOutputDescription(String name) {
    return outputDescriptions.getOrDefault(name, "");
  }

  public void output(String name, SdkBindingData value) {
    output(name, value, "");
  }

  public void output(String name, SdkBindingData value, String help) {
    outputDescriptions.put(name, help);
    outputs.put(name, value);
  }

  public WorkflowTemplate toIdlTemplate() {
    return WorkflowTemplateIdl.ofBuilder(this);
  }
}
