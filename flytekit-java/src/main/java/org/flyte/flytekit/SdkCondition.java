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

public class SdkCondition<OutputT> extends SdkTransform<OutputT> {
  private final SdkType<OutputT> outputType;
  private final List<SdkConditionCase<OutputT>> cases;
  private final String otherwiseName;
  private final SdkTransform<OutputT> otherwise;

  SdkCondition(
      List<SdkConditionCase<OutputT>> cases,
      String otherwiseName,
      SdkTransform<OutputT> otherwise) {
    this.cases = cases;
    this.otherwiseName = otherwiseName;
    this.otherwise = otherwise;
    this.outputType = cases.get(0).then().getOutputType();
  }

  public SdkCondition<OutputT> when(
      String name, SdkBooleanExpression condition, SdkTransform<OutputT> then) {

    List<SdkConditionCase<OutputT>> newCases = new ArrayList<>(cases);
    newCases.add(SdkConditionCase.create(name, condition, then));

    return new SdkCondition<>(newCases, this.otherwiseName, this.otherwise);
  }

  public SdkCondition<OutputT> otherwise(String name, SdkTransform<OutputT> otherwise) {
    if (this.otherwise != null) {
      throw new IllegalStateException("Can't set 'otherwise' because it's already set");
    }

    return new SdkCondition<>(this.cases, name, otherwise);
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

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
