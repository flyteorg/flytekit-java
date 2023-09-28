/*
 * Copyright 2020-2022 Flyte Authors
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

import static java.util.stream.Collectors.toList;
import static org.flyte.api.v1.Node.START_NODE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TypedInterface;
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

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node2, node3, node4)));

    assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_graphIsNotConnected() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of("node-2"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1)));

    assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_nodeNotFound() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));
    ExecutionNode node2 = createExecutionNode("node-2", ImmutableList.of("node-2", "node-3"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node2)));

    assertEquals("workflow graph isn't connected or has a cycle", e.getMessage());
  }

  @Test
  void testSort_duplicateNodeId() {
    ExecutionNode node1 = createExecutionNode("node-1", ImmutableList.of(START_NODE_ID));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ExecutionNodeCompiler.sort(ImmutableList.of(node1, node1)));

    assertEquals("Duplicate node id [node-1]", e.getMessage());
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
            () -> new ExecutionNodeCompiler(ExecutionContext.builder().build()).compile(node));

    assertEquals("Couldn't find task [unknownTask]", exception.getMessage());
  }

  @Test
  void testCompile_inputCollection() {
    Node node =
        Node.builder()
            .id("node")
            .taskNode(
                TaskNode.builder()
                    .referenceId(
                        PartialTaskIdentifier.builder().name("empty_runnable_task").build())
                    .build())
            .inputs(
                ImmutableList.of(
                    Binding.builder()
                        .binding(
                            BindingData.ofCollection(
                                ImmutableList.of(
                                    BindingData.ofOutputReference(
                                        OutputReference.builder()
                                            .nodeId("node-1")
                                            .var("any")
                                            .build()),
                                    BindingData.ofCollection(
                                        ImmutableList.of(
                                            BindingData.ofOutputReference(
                                                OutputReference.builder()
                                                    .nodeId("node-2")
                                                    .var("any")
                                                    .build()))),
                                    BindingData.ofMap(
                                        ImmutableMap.of(
                                            "node-3",
                                            BindingData.ofOutputReference(
                                                OutputReference.builder()
                                                    .nodeId("node-3")
                                                    .var("any")
                                                    .build()))))))
                        .var_("any")
                        .build()))
            .upstreamNodeIds(ImmutableList.of())
            .build();

    ExecutionNode execNode =
        new ExecutionNodeCompiler(
                ExecutionContext.builder()
                    .runnableTasks(ImmutableMap.of("empty_runnable_task", new EmptyRunnableTask()))
                    .build())
            .compile(node);

    assertEquals(ImmutableList.of("node-1", "node-2", "node-3"), execNode.upstreamNodeIds());
  }

  private static List<String> getNodeIds(List<ExecutionNode> nodes) {
    return nodes.stream().map(ExecutionNode::nodeId).collect(toList());
  }

  private static ExecutionNode createExecutionNode(String nodeId, List<String> upstreamNodeIds) {
    return ExecutionNode.builder()
        .nodeId(nodeId)
        .upstreamNodeIds(upstreamNodeIds)
        .runnableNode(new EmptyRunnableTask())
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
