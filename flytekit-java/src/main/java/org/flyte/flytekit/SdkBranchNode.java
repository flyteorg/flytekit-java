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
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;
import static org.flyte.flytekit.MoreCollectors.toUnmodifiableMap;

import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.NodeError;

public class SdkBranchNode extends SdkNode {
  private final String nodeId;
  private final SdkIfElseBlock ifElse;
  private final Map<String, LiteralType> outputTypes;
  private final List<String> upstreamNodeIds;

  private SdkBranchNode(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      SdkIfElseBlock ifElse,
      Map<String, LiteralType> outputTypes) {
    super(builder);

    this.nodeId = nodeId;
    this.upstreamNodeIds = upstreamNodeIds;
    this.ifElse = ifElse;
    this.outputTypes = outputTypes;
  }

  @Override
  public Map<String, SdkBindingData> getOutputs() {
    return outputTypes.entrySet().stream()
        .collect(toUnmodifiableMap(Map.Entry::getKey, this::createOutput));
  }

  private SdkBindingData createOutput(Map.Entry<String, LiteralType> entry) {
    return SdkBindingData.ofOutputReference(nodeId, entry.getKey(), entry.getValue());
  }

  /**
   * Get the sub-node of a branch node by name. All nodes dependent on the sub-node are going to be
   * skipped if the condition for the sub-node doesn't match.
   *
   * <p>Example: access output of FooTask if it's executed.
   *
   * <pre>{@code
   * SdkBranchNode branchNode = builder.apply(
   *   "branch-node",
   *   SdkConditions
   *      .when("foo", ..., new FooTask())
   *      .when("bar", ..., new BarTask())
   *      .otherwise("otherwise", ...));
   *
   * SdkNode foo = branchNode.getSubNode("foo");
   * SdkBindingData output = case1.getOutput("output");
   * }</pre>
   *
   * @param name sub-node name
   * @return sub-node
   */
  public SdkNode getSubNode(String name) {
    Map<String, SdkNode> subNodes = new HashMap<>();

    subNodes.put(ifElse.case_().thenNode().getNodeId(), ifElse.case_().thenNode());

    if (ifElse.elseNode() != null) {
      subNodes.put(ifElse.elseNode().getNodeId(), ifElse.elseNode());
    }

    ifElse.other().forEach(case_ -> subNodes.put(case_.thenNode().getNodeId(), case_.thenNode()));

    SdkNode subNode = subNodes.get(name);

    if (subNode == null) {
      String message =
          String.format("Sub-node [%s] doesn't exist among %s", name, subNodes.keySet());

      throw new IllegalArgumentException(message);
    }

    return new SdkSubNode(builder, nodeId, subNode);
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public Node toIdl() {
    NodeError nodeError =
        NodeError.builder().failedNodeId(nodeId).message("No cases matched").build();
    Map<String, Binding> extraInputs = new HashMap<>();

    @Var IfElseBlock ifElseBlock = IfBlockIdl.toIdl(ifElse, extraInputs);

    if (ifElseBlock.elseNode() == null) {
      ifElseBlock = ifElseBlock.toBuilder().error(nodeError).build();
    }

    return Node.builder()
        .id(nodeId)
        .branchNode(BranchNode.builder().ifElse(ifElseBlock).build())
        .inputs(unmodifiableList(new ArrayList<>(extraInputs.values())))
        .upstreamNodeIds(upstreamNodeIds)
        .build();
  }

  static class Builder {
    private final SdkWorkflowBuilder builder;

    private final Map<String, Map<String, SdkBindingData>> caseOutputs = new LinkedHashMap<>();
    private final List<SdkIfBlock> ifBlocks = new ArrayList<>();

    private SdkNode elseNode;
    private Map<String, LiteralType> outputTypes;

    Builder(SdkWorkflowBuilder builder) {
      this.builder = builder;
    }

    Builder addCase(SdkConditionCase case_) {
      SdkNode sdkNode = case_.then().apply(builder, case_.name(), emptyList(), emptyMap());
      Map<String, SdkBindingData> thatOutputs = sdkNode.getOutputs();
      Map<String, LiteralType> thatOutputTypes =
          thatOutputs.entrySet().stream()
              .collect(toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().type()));

      if (outputTypes != null) {
        if (!outputTypes.equals(thatOutputTypes)) {
          throw new IllegalArgumentException(
              String.format(
                  "Outputs of node [%s] didn't match with outputs of previous nodes %s, expected: [%s], but got [%s]",
                  sdkNode.getNodeId(), caseOutputs.keySet(), outputTypes, thatOutputTypes));
        }
      } else {
        outputTypes = thatOutputTypes;
      }

      Map<String, SdkBindingData> previous = caseOutputs.put(case_.name(), thatOutputs);

      if (previous != null) {
        throw new IllegalArgumentException(String.format("Duplicate case name [%s]", case_.name()));
      }

      ifBlocks.add(SdkIfBlock.create(case_.condition(), sdkNode));

      return this;
    }

    Builder addOtherwise(String name, SdkTransform otherwise) {
      if (elseNode != null) {
        throw new IllegalArgumentException("Duplicate otherwise clause");
      }

      if (caseOutputs.containsKey(name)) {
        throw new IllegalArgumentException(String.format("Duplicate case name [%s]", name));
      }

      elseNode = otherwise.apply(builder, name, emptyList(), emptyMap());
      caseOutputs.put(name, elseNode.getOutputs());

      return this;
    }

    SdkBranchNode build(String nodeId, List<String> upstreamNodeIds) {
      if (ifBlocks.isEmpty()) {
        throw new IllegalArgumentException("addCase should be called at least once");
      }

      SdkIfElseBlock ifElseBlock =
          SdkIfElseBlock.builder()
              .case_(ifBlocks.get(0))
              .other(ifBlocks.stream().skip(1).collect(toUnmodifiableList()))
              .elseNode(elseNode)
              .build();

      return new SdkBranchNode(builder, nodeId, upstreamNodeIds, ifElseBlock, outputTypes);
    }
  }

  static class SdkSubNode extends SdkNode {
    private final String parentNodeId;
    private final SdkNode underlying;

    SdkSubNode(SdkWorkflowBuilder builder, String parentNodeId, SdkNode underlying) {
      super(builder);

      this.parentNodeId = parentNodeId;
      this.underlying = underlying;
    }

    @Override
    public Map<String, SdkBindingData> getOutputs() {
      return underlying.getOutputs();
    }

    @Override
    public String getNodeId() {
      return parentNodeId + "-" + underlying.getNodeId();
    }

    @Override
    public Node toIdl() {
      throw new UnsupportedOperationException(
          "Sub-nodes are part of SdkBranchNode and can't be converted to IDL independently");
    }
  }
}
