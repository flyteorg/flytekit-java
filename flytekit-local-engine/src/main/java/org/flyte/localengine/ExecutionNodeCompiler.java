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
import static org.flyte.api.v1.Node.START_NODE_ID;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowNode.Reference;
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
      Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans) {
    List<ExecutionNode> executableNodes =
        nodes.stream()
            .map(node -> compile(node, runnableTasks, dynamicWorkflowTasks, workflows, mockLaunchPlans))
            .collect(toList());

    return sort(executableNodes);
  }

  static ExecutionNode compile(
      Node node,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans) {
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

    if (node.branchNode() != null) {
      throw new IllegalArgumentException("BranchNode isn't yet supported for local execution");
    } else if (node.workflowNode() != null) {
      return compileWorkflowNode(node, workflows, mockLaunchPlans, upstreamNodeIds);
    } else if (node.taskNode() != null) {
      return compileTaskNode(node, runnableTasks, dynamicWorkflowTasks, upstreamNodeIds);
    }

    throw new IllegalArgumentException(
        String.format("Node [%s] must be a task, branch or workflow node", node.id()));
  }

  private static ExecutionNode compileWorkflowNode(Node node, Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans, List<String> upstreamNodeIds) {
    WorkflowNode.Reference reference = node.workflowNode().reference();
    switch (reference.kind()) {
      case SUB_WORKFLOW_REF:
        return compileSubWorkflowRef(node, workflows, upstreamNodeIds, reference);
      case LAUNCH_PLAN_REF:
        return compileLaunchPlanRef(node, mockLaunchPlans, upstreamNodeIds, reference);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported Reference.Kind: [%s]", reference.kind()));
    }
  }

  private static ExecutionNode compileSubWorkflowRef(Node node, Map<String, WorkflowTemplate> workflows,
      List<String> upstreamNodeIds, Reference reference) {
    String workflowName = reference.subWorkflowRef().name();
    WorkflowTemplate workflowTemplate = workflows.get(workflowName);

    Objects.requireNonNull(
        workflowTemplate, () -> String.format("Couldn't find workflow [%s]", workflowName));

    return ExecutionNode.builder()
        .nodeId(node.id())
        .bindings(node.inputs())
        .subWorkflow(workflowTemplate)
        .upstreamNodeIds(upstreamNodeIds)
        .attempts(0)
        .build();
  }

  private static ExecutionNode compileLaunchPlanRef(Node node,
      Map<String, RunnableTask> mockLaunchPlans, List<String> upstreamNodeIds,
      Reference reference) {
    String launchPlanName = reference.launchPlanRef().name();
    // For local executions we treat launch plan references as tasks
    RunnableTask launchPlan = mockLaunchPlans.get(launchPlanName);

    Objects.requireNonNull(
        launchPlan, () -> String.format("Couldn't find launchplan [%s]", launchPlanName));
    return ExecutionNode.builder()
        .nodeId(node.id())
        .bindings(node.inputs())
        .runnableTask(launchPlan)
        .upstreamNodeIds(upstreamNodeIds)
        .attempts(1)
        .build();
  }

  private static ExecutionNode compileTaskNode(Node node, Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks, List<String> upstreamNodeIds) {
    String taskName = node.taskNode().referenceId().name();

    DynamicWorkflowTask dynamicWorkflowTask = dynamicWorkflowTasks.get(taskName);
    if (dynamicWorkflowTask != null) {
      throw new IllegalArgumentException(
          "DynamicWorkflowTask isn't yet supported for local execution");
    }

    RunnableTask runnableTask = runnableTasks.get(taskName);
    Objects.requireNonNull(
        runnableTask, () -> String.format("Couldn't find task [%s]", taskName));

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
    // would always be executed earlier if possible
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
