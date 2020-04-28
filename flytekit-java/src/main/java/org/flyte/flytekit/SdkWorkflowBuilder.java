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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Timestamp;

public class SdkWorkflowBuilder {

  private final Map<String, SdkNode> allNodes;

  SdkWorkflowBuilder() {
    this.allNodes = new HashMap<>();
  }

  public SdkNode apply(String nodeId, SdkRunnableTask<?, ?> task) {
    return apply(nodeId, task, ImmutableMap.of());
  }

  public SdkNode apply(
      String nodeId, SdkRunnableTask<?, ?> task, Map<String, SdkBindingData> inputs) {
    SdkTaskNode sdkNode = new SdkTaskNode(this, nodeId, task, inputs);
    applyInternal(sdkNode);

    return sdkNode;
  }

  public static SdkBindingData literalOf(long value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public static SdkBindingData literalOf(double value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public static SdkBindingData literalOf(String value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public static SdkBindingData literalOf(boolean value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public static SdkBindingData literalOf(Timestamp value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public static SdkBindingData literalOf(Duration value) {
    return SdkBindingData.ofScalar(Scalar.create(Primitive.of(value)));
  }

  public SdkBinding mapOf(
      String name1, SdkBindingData value1, String name2, SdkBindingData value2) {
    return new SdkBinding(this, ImmutableMap.of(name1, value1, name2, value2));
  }

  public SdkBinding tupleOf(SdkNode... nodes) {
    Map<String, SdkBindingData> inputs =
        Stream.of(nodes)
            .flatMap(x -> x.getOutputs().entrySet().stream())
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    return new SdkBinding(this, inputs);
  }

  public void applyInternal(SdkNode node) {
    allNodes.put(node.getNodeId(), node);
  }

  public List<Node> toIdl(SdkConfig config) {
    return allNodes.values().stream()
        .map(x -> x.toIdl(config))
        .collect(ImmutableList.toImmutableList());
  }
}
