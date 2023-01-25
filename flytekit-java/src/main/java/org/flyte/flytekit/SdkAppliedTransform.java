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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SdkTransform} with its inputs applied, so converting it from {@code
 * SdkTransform<OriginalInputT, OutputT>} to a {@code SdkTransform<Void, OutputT>}.
 */
class SdkAppliedTransform<OriginalInputT, OutputT> extends SdkTransform<Void, OutputT> {
  private final SdkTransform<OriginalInputT, OutputT> transform;
  private final OriginalInputT appliedInputs;

  SdkAppliedTransform(
      SdkTransform<OriginalInputT, OutputT> transform, @Nullable OriginalInputT appliedInputs) {
    transform.checkNullOnlyVoid(appliedInputs);
    this.transform = transform;
    this.appliedInputs = appliedInputs;
  }

  @Override
  public SdkType<Void> getInputType() {
    return SdkTypes.nulls();
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return transform.getOutputType();
  }

  @Override
  public String getName() {
    return transform.getName();
  }

  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {
    return transform.apply(builder, nodeId, upstreamNodeIds, metadata, appliedInputs);
  }
}
