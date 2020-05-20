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

import java.util.List;
import java.util.Map;

public class SdkBinding {
  private final SdkWorkflowBuilder builder;
  private final Map<String, SdkBindingData> bindingData;
  private final List<String> upstreamNodeIds;

  SdkBinding(
      SdkWorkflowBuilder builder,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> bindingData) {
    this.builder = builder;
    this.upstreamNodeIds = upstreamNodeIds;
    this.bindingData = bindingData;
  }

  public SdkNode apply(String nodeId, SdkTransform transform) {
    return builder.applyInternal(nodeId, transform, upstreamNodeIds, bindingData);
  }
}
