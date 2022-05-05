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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.api.v1.Node.START_NODE_ID;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;

/**
 * Performs following operations
 *
 * <ul>
 *   <li>Task identifier is resolved to RunnableTask.
 *   <li>Upstream node ids are computed from bindings and original node ids.
 *   <li>All upstream nodes exist and there are no cycles.
 *   <li>Execution nodes are topologically sorted.
 *   <li>All nodes are connected to other nodes, or start-node.
 *   <li>TODO type checking
 *   <li>TODO type conversion
 * </ul>
 */
class ExecutionNodeCompiler {

  /**
   * Given a list of flytekit-api nodes, validates them, and determines their sequential execution
   * order.
   *
   * @param nodes nodes
   * @param runnableTasks runnable tasks
   * @param dynamicWorkflowTasks dynamic workflow tasks
   * @param workflows workflow templates
   * @return execution nodes
   */
  static List<ExecutionNode> compile(
      List<Node> nodes,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflows) {
    List<ExecutionNode> executableNodes =
        nodes.stream()
            .flatMap(node -> expand(node, workflows).stream())
            .map(node -> compile(node, runnableTasks, dynamicWorkflowTasks))
            .collect(toList());

    return sort(executableNodes);
  }

  static ExecutionNode compile(
      Node node,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks) {
    List<String> upstreamNodeIds = new ArrayList<>();

    node.inputs().stream()
        .map(Binding::binding)
        .flatMap(ExecutionNodeCompiler::unpackBindingData)
        .filter(x -> x.kind() == BindingData.Kind.PROMISE)
        .map(x -> x.promise().nodeId())
        .forEach(upstreamNodeIds::add);

    upstreamNodeIds.addAll(node.upstreamNodeIds());

    if (upstreamNodeIds.isEmpty()) {
      upstreamNodeIds.add(START_NODE_ID);
    }

    if (node.workflowNode() != null) {
      throw new IllegalArgumentException("WorkflowNode must be expanded");
    }

    if (node.branchNode() != null) {
      throw new IllegalArgumentException("BranchNode isn't yet supported for local execution");
    }

    String taskName = node.taskNode().referenceId().name();
    DynamicWorkflowTask dynamicWorkflowTask = dynamicWorkflowTasks.get(taskName);
    RunnableTask runnableTask = runnableTasks.get(taskName);

    if (dynamicWorkflowTask != null) {
      throw new IllegalArgumentException(
          "DynamicWorkflowTask isn't yet supported for local execution");
    }

    Objects.requireNonNull(runnableTask, () -> String.format("Couldn't find task [%s]", taskName));

    int attempts = runnableTask.getRetries().retries() + 1;

    return ExecutionNode.builder()
        .nodeId(node.id())
        .bindings(node.inputs())
        .runnableTask(runnableTask)
        .upstreamNodeIds(upstreamNodeIds)
        .attempts(attempts)
        .build();
  }

  /**
   * Performs topological sorting with BFS. In case of ambiguity, nodes closer to root take
   * priority. If two nodes have the same depth, relative order in input list is used. Because of
   * that, the implementation is slightly different from what you would normally see for topological
   * sorting.
   *
   * @param nodes nodes
   * @return execution nodes
   */
  static List<ExecutionNode> sort(List<ExecutionNode> nodes) {
    // priority is initial order in the list, node earlier in the list
    // would be always executed earlier if possible
    Map<String, Integer> priorityMap = new HashMap<>();
    Map<String, Integer> degreeMap = new HashMap<>();
    Map<String, ExecutionNode> lookup = new HashMap<>();
    Map<String, List<String>> downstreamNodeIdsMap = new HashMap<>();

    for (int i = 0; i < nodes.size(); i++) {
      ExecutionNode node = nodes.get(i);

      priorityMap.put(node.nodeId(), i);
      degreeMap.put(node.nodeId(), node.upstreamNodeIds().size());

      for (String upstreamNodeId : node.upstreamNodeIds()) {
        downstreamNodeIdsMap.putIfAbsent(upstreamNodeId, new ArrayList<>());
        downstreamNodeIdsMap.get(upstreamNodeId).add(node.nodeId());
      }

      ExecutionNode previous = lookup.put(node.nodeId(), node);

      if (previous != null) {
        throw new IllegalArgumentException(String.format("Duplicate node id [%s]", node.nodeId()));
      }
    }

    Deque<List<String>> deque = new ArrayDeque<>();
    Set<String> visitedNodeIds = new HashSet<>();
    List<ExecutionNode> topologicallySorted = new ArrayList<>();

    deque.add(singletonList(START_NODE_ID));

    while (!deque.isEmpty()) {
      List<String> nodeIds = deque.pollFirst();
      List<String> downstreamNodeIds = new ArrayList<>();

      for (String nodeId : nodeIds) {
        if (!nodeId.equals(START_NODE_ID)) {
          ExecutionNode node = lookup.get(nodeId);
          Objects.requireNonNull(node, () -> String.format("node not found [%s]", nodeId));
          topologicallySorted.add(node);
        }

        boolean visited = visitedNodeIds.contains(nodeId);

        if (visited) {
          throw new IllegalStateException("invariant failed");
        }

        for (String downstreamNodeId : downstreamNodeIdsMap.getOrDefault(nodeId, emptyList())) {
          int newDegree = degreeMap.get(downstreamNodeId) - 1;

          if (newDegree == 0) {
            downstreamNodeIds.add(downstreamNodeId);
          }

          degreeMap.put(downstreamNodeId, newDegree);
        }
      }

      visitedNodeIds.addAll(nodeIds);

      // traverse each batch of nodes in priority order
      if (!downstreamNodeIds.isEmpty()) {
        List<String> sortedDownstreamNodeIds =
            downstreamNodeIds.stream()
                .sorted(Comparator.comparing(priorityMap::get))
                .distinct()
                .collect(toList());

        deque.push(sortedDownstreamNodeIds);
      }
    }

    if (nodes.size() != topologicallySorted.size()) {
      throw new IllegalArgumentException("workflow graph isn't connected or has a cycle");
    }

    return topologicallySorted;
  }

  static List<Node> expand(Node node, Map<String, WorkflowTemplate> workflows) {
    if (node.workflowNode() != null) {
      WorkflowNode.Reference reference = node.workflowNode().reference();
      switch (reference.kind()) {
        case SUB_WORKFLOW_REF:
          String workflowName = reference.subWorkflowRef().name();
          WorkflowTemplate workflowTemplate = workflows.get(workflowName);

          Objects.requireNonNull(
              workflowTemplate, () -> String.format("Couldn't find workflow [%s]", workflowName));

          // alter the template nodes, so we prefix and remap the inputs
          return workflowTemplate.nodes().stream()
              .map(
                  n ->
                      n.toBuilder()
                          .id(node.id() + "-" + n.id())
                          .inputs(remapBindings(node.inputs(), n.inputs()))
                          .build())
              .collect(toList());
        case LAUNCH_PLAN_REF:
          throw new IllegalArgumentException(
              "LaunchPlanRef isn't yet supported for local execution");
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported Reference.Kind: [%s]", reference.kind()));
      }
    } else {
      return Collections.singletonList(node);
    }
  }

  static List<Binding> remapBindings(List<Binding> startNodeBindings, List<Binding> bindings) {
    Map<String, BindingData> startNodeBindingData =
        startNodeBindings.stream().collect(Collectors.toMap(Binding::var_, Binding::binding));
    return bindings.stream()
        .map(
            b ->
                Binding.builder()
                    .var_(b.var_())
                    .binding(remapBindingData(startNodeBindingData, b.binding()))
                    .build())
        .collect(toList());
  }

  static BindingData remapBindingData(
      Map<String, BindingData> startNodeBindingData, BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return bindingData;
      case COLLECTION:
        return BindingData.ofCollection(
            bindingData.collection().stream()
                .map(b -> remapBindingData(startNodeBindingData, b))
                .collect(toList()));
      case PROMISE:
        String nodeId = bindingData.promise().nodeId();
        if (nodeId.equals(START_NODE_ID)) {
          return startNodeBindingData.get(bindingData.promise().var());
        } else {
          return bindingData;
        }
      case MAP:
        return BindingData.ofMap(
            bindingData.map().entrySet().stream()
                .map(
                    entry ->
                        new AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey(),
                            remapBindingData(startNodeBindingData, entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    throw new AssertionError("Unexpected BindingData.Kind: " + bindingData.kind());
  }

  private static Stream<BindingData> unpackBindingData(BindingData bindingData) {
    if (bindingData.kind() == BindingData.Kind.COLLECTION) {
      return bindingData.collection().stream().flatMap(ExecutionNodeCompiler::unpackBindingData);
    } else if (bindingData.kind() == BindingData.Kind.MAP) {
      return bindingData.map().values().stream().flatMap(ExecutionNodeCompiler::unpackBindingData);
    } else {
      return Stream.of(bindingData);
    }
  }
}
