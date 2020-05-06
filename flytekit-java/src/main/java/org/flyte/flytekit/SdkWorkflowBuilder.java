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

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
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
    return apply(nodeId, task, Collections.emptyMap());
  }

  public SdkNode apply(String nodeId, SdkTransform transform, Map<String, SdkBindingData> inputs) {
    SdkNode sdkNode = transform.apply(this, nodeId, inputs);
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

  public SdkBinding mapOf(String name1, SdkBindingData value1) {
    return new SdkBinding(this, singletonMap(name1, value1));
  }

  public SdkBinding mapOf(
      String name1, SdkBindingData value1, String name2, SdkBindingData value2) {
    Map<String, SdkBindingData> map = new HashMap<>();
    map.put(name1, value1);
    map.put(name2, value2);
    return new SdkBinding(this, unmodifiableMap(map));
  }

  public SdkBinding mapOf(
      String name1,
      SdkBindingData value1,
      String name2,
      SdkBindingData value2,
      String name3,
      SdkBindingData value3) {
    Map<String, SdkBindingData> map = new HashMap<>();
    map.put(name1, value1);
    map.put(name2, value2);
    map.put(name3, value3);
    return new SdkBinding(this, unmodifiableMap(map));
  }

  public SdkBinding tupleOf(SdkNode... nodes) {
    Map<String, SdkBindingData> inputs =
        Stream.of(nodes)
            .flatMap(x -> x.getOutputs().entrySet().stream())
            .collect(
                collectingAndThen(
                    toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap));

    return new SdkBinding(this, inputs);
  }

  public void applyInternal(SdkNode node) {
    allNodes.put(node.getNodeId(), node);
  }

  public List<Node> toIdl() {
    return allNodes.values().stream()
        .map(SdkNode::toIdl)
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
  }
}
