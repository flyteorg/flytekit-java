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

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.NodeError;

/** A node in the workflow denoting a branching decision in the DAG. */
public class SdkBranchNode<OutputT> extends SdkNode<OutputT> {
  private final String nodeId;
  private final SdkIfElseBlock ifElse;
  private final Map<String, SdkLiteralType<?>> outputTypes;
  private final List<String> upstreamNodeIds;

  private final OutputT outputs;

  private SdkBranchNode(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      SdkIfElseBlock ifElse,
      Map<String, SdkLiteralType<?>> outputTypes,
      OutputT outputs) {
    super(builder);

    this.nodeId = nodeId;
    this.upstreamNodeIds = upstreamNodeIds;
    this.ifElse = ifElse;
    this.outputTypes = outputTypes;
    this.outputs = outputs;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, SdkBindingData<?>> getOutputBindings() {
    return outputTypes.entrySet().stream()
        .collect(toUnmodifiableMap(Map.Entry::getKey, this::createOutput));
  }

  /** {@inheritDoc} */
  @Override
  public OutputT getOutputs() {
    return outputs;
  }

  private SdkBindingData<?> createOutput(Map.Entry<String, SdkLiteralType<?>> entry) {
    return SdkBindingData.promise(entry.getValue(), nodeId, entry.getKey());
  }

  /** {@inheritDoc} */
  @Override
  public String getNodeId() {
    return nodeId;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The returned Node contains a branch node
   */
  @Override
  public Node toIdl() {
    NodeError nodeError =
        NodeError.builder().failedNodeId(nodeId).message("No cases matched").build();
    Map<String, Binding> extraInputs = new HashMap<>();

    @Var IfElseBlock ifElseBlock = IfBlockIdl.toIdl(ifElse, extraInputs);

    if (ifElseBlock.elseNode() == null) {
      ifElseBlock = ifElseBlock.toBuilder().error(nodeError).build();
    }

    // inputs in var order for predictability
    List<Binding> inputs =
        extraInputs.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(Entry::getValue)
            .collect(toUnmodifiableList());
    return Node.builder()
        .id(nodeId)
        .branchNode(BranchNode.builder().ifElse(ifElseBlock).build())
        .inputs(inputs)
        .upstreamNodeIds(upstreamNodeIds)
        .build();
  }

  static class Builder<OutputT> {
    private final SdkWorkflowBuilder builder;
    private final SdkType<OutputT> outputType;

    private final Map<String, Map<String, SdkBindingData<?>>> caseOutputs = new LinkedHashMap<>();
    private final List<SdkIfBlock> ifBlocks = new ArrayList<>();

    private SdkNode<?> elseNode;
    private Map<String, SdkLiteralType<?>> outputTypes;

    Builder(SdkWorkflowBuilder builder, SdkType<OutputT> outputType) {
      this.builder = builder;
      this.outputType = outputType;
    }

    @CanIgnoreReturnValue
    Builder<OutputT> addCase(SdkConditionCase<OutputT> case_) {
      SdkNode<OutputT> sdkNode =
          case_.then().apply(builder, case_.name(), List.of(), /*metadata=*/ null, Map.of());

      Map<String, SdkBindingData<?>> thatOutputs = sdkNode.getOutputBindings();
      Map<String, SdkLiteralType<?>> thatOutputTypes =
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

      Map<String, SdkBindingData<?>> previous = caseOutputs.put(case_.name(), thatOutputs);

      if (previous != null) {
        throw new IllegalArgumentException(String.format("Duplicate case name [%s]", case_.name()));
      }

      ifBlocks.add(SdkIfBlock.create(case_.condition(), sdkNode));

      return this;
    }

    @CanIgnoreReturnValue
    Builder<OutputT> addOtherwise(String name, SdkTransform<Void, OutputT> otherwise) {
      if (elseNode != null) {
        throw new IllegalArgumentException("Duplicate otherwise clause");
      }

      if (caseOutputs.containsKey(name)) {
        throw new IllegalArgumentException(String.format("Duplicate case name [%s]", name));
      }

      elseNode = otherwise.apply(builder, name, List.of(), /*metadata=*/ null, Map.of());
      caseOutputs.put(name, elseNode.getOutputBindings());

      return this;
    }

    SdkBranchNode<OutputT> build(String nodeId, List<String> upstreamNodeIds) {
      if (ifBlocks.isEmpty()) {
        throw new IllegalArgumentException("addCase should be called at least once");
      }

      SdkIfElseBlock ifElseBlock =
          SdkIfElseBlock.builder()
              .case_(ifBlocks.get(0))
              .other(ifBlocks.stream().skip(1).collect(toUnmodifiableList()))
              .elseNode(elseNode)
              .build();

      OutputT outputs = outputType.promiseFor(nodeId);
      return new SdkBranchNode<>(
          builder, nodeId, upstreamNodeIds, ifElseBlock, outputTypes, outputs);
    }
  }
}
