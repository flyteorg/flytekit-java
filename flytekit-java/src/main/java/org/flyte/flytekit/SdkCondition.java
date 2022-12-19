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

public class SdkCondition<NamedOutputT extends NamedOutput> extends SdkTransform<NamedOutputT> {
  private final List<SdkConditionCase<NamedOutputT>> cases;
  private final String otherwiseName;
  private final SdkTransform<NamedOutputT> otherwise;

  SdkCondition(
      List<SdkConditionCase<NamedOutputT>> cases,
      String otherwiseName,
      SdkTransform<NamedOutputT> otherwise) {
    this.cases = cases;
    this.otherwiseName = otherwiseName;
    this.otherwise = otherwise;
  }

  public SdkCondition<NamedOutputT> when(
      String name, SdkBooleanExpression condition, SdkTransform<NamedOutputT> then) {
    List<SdkConditionCase<NamedOutputT>> newCases = new ArrayList<>(cases);
    newCases.add(SdkConditionCase.create(name, condition, then));

    return new SdkCondition<>(newCases, this.otherwiseName, this.otherwise);
  }

  public SdkCondition<NamedOutputT> otherwise(String name, SdkTransform<NamedOutputT> otherwise) {
    if (this.otherwise != null) {
      throw new IllegalStateException("Can't set 'otherwise' because it's already set");
    }

    return new SdkCondition<>(this.cases, name, otherwise);
  }

  @Override
  public SdkNode<NamedOutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {
    if (metadata != null) {
      throw new IllegalArgumentException("invariant failed: metadata must be null");
    }
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("invariant failed: inputs must be empty");
    }

    SdkBranchNode.Builder<NamedOutputT> nodeBuilder = new SdkBranchNode.Builder<>(builder);

    for (SdkConditionCase case_ : cases) {
      nodeBuilder.addCase(case_);
    }

    if (otherwiseName != null) {
      nodeBuilder.addOtherwise(otherwiseName, otherwise);
    }

    return nodeBuilder.build(nodeId, upstreamNodeIds);
  }
}
