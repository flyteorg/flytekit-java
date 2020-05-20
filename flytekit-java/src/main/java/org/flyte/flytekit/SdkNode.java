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

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Node;

/** Represent a node in the workflow DAG. */
public abstract class SdkNode {

  protected final SdkWorkflowBuilder builder;

  protected SdkNode(SdkWorkflowBuilder builder) {
    this.builder = builder;
  }

  public abstract Map<String, SdkBindingData> getOutputs();

  public SdkBindingData getOutput(String name) {
    SdkBindingData output = getOutputs().get(name);

    requireNonNull(output, String.format("output not found [%s]", name));

    return output;
  }

  public abstract String getNodeId();

  public abstract Node toIdl();

  public SdkNode apply(String id, SdkRunnableTask<?, ?> task) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputs().isEmpty() ? Collections.singletonList(getNodeId()) : Collections.emptyList();

    return builder.applyInternal(id, task, upstreamNodeIds, getOutputs());
  }
}
