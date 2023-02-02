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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Node;

/** Represent a node in the workflow DAG. */
public abstract class SdkNode<OutputT> {

  protected final SdkWorkflowBuilder builder;

  protected SdkNode(SdkWorkflowBuilder builder) {
    this.builder = builder;
  }

  /**
   * Returns output of this node as the bindings map.
   *
   * @return output binding map.
   */
  public abstract Map<String, SdkBindingData<?>> getOutputBindings();

  /**
   * Returns output of this node as an {@link OutputT}.
   *
   * @return output.
   */
  public abstract OutputT getOutputs();

  /**
   * Returns the id of this node. Nodes ids should be unique in the workflow DAG.
   *
   * @return the id.
   */
  public abstract String getNodeId();

  /**
   * Returns the idl representation of this node.
   *
   * @return the idl representation.
   */
  public abstract Node toIdl();

  // TODO we need a version with no nodeId for consistency with builder
  /**
   * Returns a new node resulting from applying the specified transform to this node.
   *
   * @param nodeId the node id for the new node
   * @param transform the transform to apply to this node. The input o the transform should have
   *     {@link OutputT} as input type and {@link NewOutputT} as output type
   * @return the new node.
   */
  public <NewOutputT> SdkNode<NewOutputT> apply(
      String nodeId, SdkTransform<OutputT, NewOutputT> transform) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputBindings().isEmpty()
            ? Collections.singletonList(getNodeId())
            : Collections.emptyList();

    return builder.applyInternal(nodeId, transform, upstreamNodeIds, getOutputs());
  }
}
