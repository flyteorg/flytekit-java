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
package org.flyte.jflyte;

import static org.flyte.api.v1.Node.START_NODE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TypedInterface;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class ExecutionNodeCompilerTest {

  @Test
  void testSort() {
    //                     + node-3 +
    //                    /          \
    // node-1 - node-2 -+             +-- node-5
    //                    \          /
    //                     + node-4 +-- node-6
    //

    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));
    ExecutionNode node2 = createExecutionNode("node-2", ImmutableList.of("node-1"));
    ExecutionNode node3 = createExecutionNode("node-3", ImmutableList.of("node-2"));
    ExecutionNode node4 = createExecutionNode("node-4", ImmutableList.of("node-2"));
    ExecutionNode node5 = createExecutionNode("node-5", ImmutableList.of("node-3", "node-4"));
    ExecutionNode node6 = createExecutionNode("node-6", ImmutableList.of("node-4"));

    List<ExecutionNode> sorted =
        ExecutionNodeCompiler.sort(ImmutableList.of(node6, node5, node4, node3, node2, node1));

    assertEquals(
        ImmutableList.of("node-1", "node-2", "node-4", "node-3", "node-6", "node-5"),
        getNodeIds(sorted));
  }

  @Test
  void testSort_cycle() {
    // node-1 -> node-2 -> node-3 -> node-4
    //             ^                   |
    //             +-------------------+

    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));
    ExecutionNode node2 = createExecutionNode("node-2", ImmutableList.of("node-1", "node-4"));
    ExecutionNode node3 = createExecutionNode("node-3", ImmutableList.of("node-2"));
    ExecutionNode node4 = createExecutionNode("node-4", ImmutableList.of("node-3"));

    VerifyException e =
        assertThrows(
            VerifyException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node2, node3, node4)));

    Assert.assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_graphIsNotConnected() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of("node-2"));

    VerifyException e =
        assertThrows(
            VerifyException.class, () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1)));

    Assert.assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_nodeNotFound() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));
    ExecutionNode node2 = createExecutionNode("node-2", ImmutableList.of("node-2", "node-3"));

    VerifyException e =
        assertThrows(
            VerifyException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node2)));

    Assert.assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_duplicateNodeId() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));

    VerifyException e =
        assertThrows(
            VerifyException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node1)));

    Assert.assertEquals("duplicate node id [node-1]", e.getMessage());
  }

  @Test
  void testSort_notConnected() {
    // node-1 - node-2
    // node-3 - node-4

    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));
    ExecutionNode node2 = createExecutionNode("node-2", ImmutableList.of("node-1"));
    ExecutionNode node3 = createExecutionNode("node-3", ImmutableList.of(START_NODE_ID));
    ExecutionNode node4 = createExecutionNode("node-4", ImmutableList.of("node-3"));

    List<ExecutionNode> sorted =
        ExecutionNodeCompiler.sort(ImmutableList.of(node4, node1, node2, node3));

    assertEquals(ImmutableList.of("node-1", "node-3", "node-4", "node-2"), getNodeIds(sorted));
  }

  @Test
  void testCompile_unknownTask() {
    Node node = createNode("node-1", ImmutableList.of(START_NODE_ID));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> ExecutionNodeCompiler.compile(node, Collections.emptyMap()));

    assertEquals("Couldn't find task named: [unknownTask]", exception.getMessage());
  }

  private static List<String> getNodeIds(List<ExecutionNode> nodes) {
    return nodes.stream().map(ExecutionNode::nodeId).collect(Collectors.toList());
  }

  private static ExecutionNode createExecutionNode(String nodeId, List<String> upstreamNodeIds) {
    return ExecutionNode.builder()
        .nodeId(nodeId)
        .upstreamNodeIds(upstreamNodeIds)
        .runnableTask(new EmptyRunnableTask())
        .bindings(ImmutableList.of())
        .attempts(1)
        .build();
  }

  private static Node createNode(String nodeId, List<String> upstreamNodeIds) {
    return Node.builder()
        .id(nodeId)
        .taskNode(
            TaskNode.builder()
                .referenceId(PartialTaskIdentifier.builder().name("unknownTask").build())
                .build())
        .upstreamNodeIds(upstreamNodeIds)
        .inputs(ImmutableList.of())
        .build();
  }

  private static class EmptyRunnableTask implements RunnableTask {

    @Override
    public String getName() {
      return "empty_runnable_task";
    }

    @Override
    public TypedInterface getInterface() {
      return TypedInterface.builder().inputs(ImmutableMap.of()).outputs(ImmutableMap.of()).build();
    }

    @Override
    public Map<String, Literal> run(Map<String, Literal> inputs) {
      return ImmutableMap.of();
    }

    @Override
    public RetryStrategy getRetries() {
      return RetryStrategy.builder().retries(0).build();
    }
  }
}
