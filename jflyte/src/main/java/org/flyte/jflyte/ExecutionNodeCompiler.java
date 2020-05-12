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

import static org.flyte.jflyte.ExecutionNode.START_NODE_ID;

import com.google.common.base.Verify;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.RunnableTask;

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
   */
  static List<ExecutionNode> compile(List<Node> nodes, Map<String, RunnableTask> runnableTasks) {
    List<ExecutionNode> executableNodes =
        nodes.stream().map(node -> compile(node, runnableTasks)).collect(Collectors.toList());

    return sort(executableNodes);
  }

  static ExecutionNode compile(Node node, Map<String, RunnableTask> runnableTasks) {
    List<String> upstreamNodeIds = new ArrayList<>();

    node.inputs().stream()
        .filter(x -> x.binding().kind() == BindingData.Kind.PROMISE)
        .map(x -> x.binding().promise().nodeId())
        .forEach(upstreamNodeIds::add);

    upstreamNodeIds.addAll(node.upstreamNodeIds());

    if (upstreamNodeIds.isEmpty()) {
      upstreamNodeIds.add(START_NODE_ID);
    }

    RunnableTask runnableTask = runnableTasks.get(node.taskNode().referenceId().name());

    return ExecutionNode.builder()
        .nodeId(node.id())
        .bindings(node.inputs())
        .runnableTask(runnableTask)
        .upstreamNodeIds(upstreamNodeIds)
        .build();
  }

  /**
   * Performs topological sorting with BFS. In case of ambiguity, nodes closer to root take
   * priority. If two nodes have the same depth, relative order in input list is used. Because of
   * that, the implementation is slightly different from what you would normally see for topological
   * sorting.
   */
  static List<ExecutionNode> sort(List<ExecutionNode> nodes) {
    // priority is initial order in the list, node earlier in the list
    // would be always executed earlier if possible
    Map<String, Integer> priorityMap = new HashMap<>();
    Map<String, Integer> degreeMap = new HashMap<>();
    Map<String, ExecutionNode> lookup = new HashMap<>();
    ListMultimap<String, String> downstreamNodeIdsMap = ArrayListMultimap.create();

    for (int i = 0; i < nodes.size(); i++) {
      ExecutionNode node = nodes.get(i);

      priorityMap.put(node.nodeId(), i);
      degreeMap.put(node.nodeId(), node.upstreamNodeIds().size());

      for (String upstreamNodeId : node.upstreamNodeIds()) {
        downstreamNodeIdsMap.put(upstreamNodeId, node.nodeId());
      }

      ExecutionNode previous = lookup.put(node.nodeId(), node);
      Verify.verify(previous == null, "duplicate node id [%s]", node.nodeId());
    }

    Deque<List<String>> deque = new ArrayDeque<>();
    Set<String> visitedNodeIds = new HashSet<>();
    List<ExecutionNode> topologicallySorted = new ArrayList<>();

    deque.add(downstreamNodeIdsMap.get(START_NODE_ID));

    while (!deque.isEmpty()) {
      List<String> nodeIds = deque.pollFirst();
      List<String> downstreamNodeIds = new ArrayList<>();

      for (String nodeId : nodeIds) {
        ExecutionNode node = lookup.get(nodeId);

        Verify.verifyNotNull(node, "node not found [%s]", nodeId);

        boolean visited = visitedNodeIds.contains(nodeId);

        Verify.verify(!visited, "invariant failed");

        for (String downstreamNodeId : downstreamNodeIdsMap.get(nodeId)) {
          int newDegree = degreeMap.get(downstreamNodeId) - 1;

          if (newDegree == 0) {
            downstreamNodeIds.add(downstreamNodeId);
          }

          degreeMap.put(downstreamNodeId, newDegree);
        }

        topologicallySorted.add(node);
      }

      visitedNodeIds.addAll(nodeIds);

      // traverse each batch of nodes in priority order
      if (!downstreamNodeIds.isEmpty()) {
        List<String> sortedDownstreamNodeIds =
            downstreamNodeIds.stream()
                .sorted(Comparator.comparing(priorityMap::get))
                .distinct()
                .collect(Collectors.toList());

        deque.push(sortedDownstreamNodeIds);
      }
    }

    Verify.verify(
        nodes.size() == topologicallySorted.size(),
        "workflow graph isn't connected or has a cycle");

    return topologicallySorted;
  }
}
