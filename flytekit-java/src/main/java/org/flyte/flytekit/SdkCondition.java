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

public class SdkCondition extends SdkTransform {
  private final List<SdkConditionCase> cases;
  private final String otherwiseName;
  private final SdkTransform otherwise;

  SdkCondition(List<SdkConditionCase> cases, String otherwiseName, SdkTransform otherwise) {
    this.cases = cases;
    this.otherwiseName = otherwiseName;
    this.otherwise = otherwise;
  }

  public SdkCondition when(String name, SdkBooleanExpression condition, SdkTransform then) {
    List<SdkConditionCase> newCases = new ArrayList<>(cases);
    newCases.add(SdkConditionCase.create(name, condition, then));

    return new SdkCondition(newCases, this.otherwiseName, this.otherwise);
  }

  public SdkCondition otherwise(String name, SdkTransform otherwise) {
    if (this.otherwise != null) {
      throw new IllegalStateException("Can't set 'otherwise' because it's already set");
    }

    return new SdkCondition(this.cases, name, otherwise);
  }

  @Override
  public <T extends TypedOutput> SdkBranchNode<T> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs,
      Class<T> typedOutputClass) {
    if (metadata != null) {
      throw new IllegalArgumentException("invariant failed: metadata must be null");
    }
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("invariant failed: inputs must be empty");
    }

    SdkBranchNode.Builder<T> nodeBuilder = new SdkBranchNode.Builder<>(builder, typedOutputClass);

    for (SdkConditionCase case_ : cases) {
      nodeBuilder.addCase(case_);
    }

    if (otherwiseName != null) {
      nodeBuilder.addOtherwise(otherwiseName, otherwise);
    }

    return nodeBuilder.build(nodeId, upstreamNodeIds);
  }
}
