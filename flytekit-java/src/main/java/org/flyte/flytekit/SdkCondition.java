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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Denotes a conditional transformation. It contains several case clauses and a otherwise clause to
 * be executed if non of the conditions of the case clauses was satisfied. All the case clauses and
 * otherwise must have the same output type.
 *
 * @param <OutputT> The output type of the transformation.
 */
public class SdkCondition<OutputT> extends SdkTransform<Void, OutputT> {
  private final SdkType<OutputT> outputType;
  private final List<SdkConditionCase<OutputT>> cases;
  private final String otherwiseName;
  private final SdkTransform<Void, OutputT> otherwise;

  SdkCondition(
      List<SdkConditionCase<OutputT>> cases,
      String otherwiseName,
      SdkTransform<Void, OutputT> otherwise) {
    if (cases.isEmpty()) {
      throw new IllegalArgumentException("Empty cases on SdkCondition");
    }
    this.cases = List.copyOf(cases);
    this.otherwiseName = otherwiseName;
    this.otherwise = otherwise;

    var firstCase = cases.get(0);
    this.outputType = firstCase.then().getOutputType();
  }

  /**
   * Adds a case clause to this conditional transformation. The {@code then} transformation takes
   * no inputs.
   *
   * @param name for the case, it should be unique among all the case clauses of the condition
   * @param condition boolean expression that must evaluate to true to apply {@code then}
   *     transformation.
   * @param then the transformation to apply if {@code condition} evaluates to true. The
   *     transformation must receive no inputs.
   * @return this modified transformation.
   */
  public SdkCondition<OutputT> when(
      String name, SdkBooleanExpression condition, SdkTransform<Void, OutputT> then) {
    var newCases = new ArrayList<>(cases);
    newCases.add(SdkConditionCase.create(name, condition, then));

    return new SdkCondition<>(newCases, this.otherwiseName, this.otherwise);
  }

  /**
   * Adds a case clause to this conditional transformation. The {@code then} transformation takes
   * {@link InputT} inputs and they are supplied as well.
   *
   * @param name for the case, it should be unique among all the case clauses of the condition
   * @param condition boolean expression that must evaluate to true to apply {@code then} transformation.
   * @param then the transformation to apply if {@code condition} evaluates to true.
   * @param inputs the inputs to apply to the {@code then} transformation if {@code condition} evaluates to true.
   * @param <InputT> the input type of the {@code then} transformation
   * @return this modified transformation.
   */
  public <InputT> SdkCondition<OutputT> when(
      String name,
      SdkBooleanExpression condition,
      SdkTransform<InputT, OutputT> then,
      InputT inputs) {
    return when(name, condition, new SdkAppliedTransform<>(then, inputs));
  }

  /**
   * Adds a otherwise clause to this conditional transformation. The {@code otherwise} transformation takes
   * no inputs.
   *
   * @param name for the case, it should be unique among all the case clauses of the condition
   * @param otherwise the transformation to apply if no other case in this transformation is true.
   * @return this modified transformation.
   */
  public SdkCondition<OutputT> otherwise(String name, SdkTransform<Void, OutputT> otherwise) {
    if (this.otherwise != null) {
      throw new IllegalStateException("Can't set 'otherwise' because it's already set");
    }

    return new SdkCondition<>(this.cases, name, otherwise);
  }

  /**
   * Adds a otherwise clause to this conditional transformation. The {@code then} transformation takes
   * {@link InputT} inputs and they are supplied as well.
   *
   * @param name for the case, it should be unique among all the case clauses of the condition
   * @param otherwise the transformation to apply if no other case in this transformation is true.
   * @param inputs the inputs to apply to the {@code then} transformation if {@code condition} evaluates to true.
   * @param <InputT> the input type of the {@code then} transformation
   * @return this modified transformation.
   */
  public <InputT> SdkCondition<OutputT> otherwise(
      String name, SdkTransform<InputT, OutputT> otherwise, InputT inputs) {
    return otherwise(name, new SdkAppliedTransform<>(otherwise, inputs));
  }

  /** {@inheritDoc} */
  @Override
  public SdkType<Void> getInputType() {
    return SdkTypes.nulls();
  }

  /** {@inheritDoc} */
  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  /**
   * {@inheritDoc}
   * Returns a {@link SdkBranchNode}
   */
  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {
    SdkBranchNode.Builder<OutputT> nodeBuilder = new SdkBranchNode.Builder<>(builder, outputType);

    for (SdkConditionCase<OutputT> case_ : cases) {
      nodeBuilder.addCase(case_);
    }

    if (otherwiseName != null) {
      nodeBuilder.addOtherwise(otherwiseName, otherwise);
    }

    return nodeBuilder.build(nodeId, upstreamNodeIds);
  }
}
