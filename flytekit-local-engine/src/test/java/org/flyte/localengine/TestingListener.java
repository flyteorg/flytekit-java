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
package org.flyte.localengine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;

public class TestingListener implements ExecutionListener {
  final List<List<Object>> actions = new ArrayList<>();

  @Override
  public void pending(ExecutionNode node) {
    actions.add(ofPending(node.nodeId()));
  }

  @Override
  public void retrying(ExecutionNode node, Map<String, Literal> inputs, Throwable e, int attempt) {
    actions.add(ofRetrying(node.nodeId(), inputs, e.getMessage(), attempt));
  }

  @Override
  public void error(ExecutionNode node, Map<String, Literal> inputs, Throwable e) {
    actions.add(ofError(node.nodeId(), inputs, e.getMessage()));
  }

  @Override
  public void starting(ExecutionNode node, Map<String, Literal> inputs) {
    actions.add(ofStarting(node.nodeId(), inputs));
  }

  @Override
  public void completed(
      ExecutionNode node, Map<String, Literal> inputs, Map<String, Literal> outputs) {
    actions.add(ofCompleted(node.nodeId(), inputs, outputs));
  }

  // we should have created a new type of each case, but it was too much boilerplate

  static List<Object> ofStarting(String nodeId, Map<String, Literal> inputs) {
    return ImmutableList.of("starting", nodeId, inputs);
  }

  static List<Object> ofRetrying(
      String nodeId, Map<String, Literal> inputs, String message, int attempt) {
    return ImmutableList.of("retrying", nodeId, inputs, message, attempt);
  }

  static List<Object> ofCompleted(
      String nodeId, Map<String, Literal> inputs, Map<String, Literal> outputs) {
    return ImmutableList.of("completed", nodeId, inputs, outputs);
  }

  static List<Object> ofError(String nodeId, Map<String, Literal> inputs, String message) {
    return ImmutableList.of("error", nodeId, inputs, message);
  }

  static List<Object> ofPending(String nodeId) {
    return ImmutableList.of("pending", nodeId);
  }
}
