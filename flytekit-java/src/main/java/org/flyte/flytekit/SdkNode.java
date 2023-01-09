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

  public abstract Map<String, SdkBindingData<?>> getOutputBindings();

  public abstract OutputT getOutputs();

  public SdkBindingData<?> getOutput(String name) {

    @SuppressWarnings("unchecked")
    SdkBindingData<?> output = getOutputBindings().get(name);

    if (output == null) {
      String message = String.format("Variable [%s] not found on node [%s].", name, getNodeId());
      CompilerError error =
          CompilerError.create(
              CompilerError.Kind.VARIABLE_NAME_NOT_FOUND,
              /* nodeId= */ getNodeId(),
              /* message= */ message);

      throw new CompilerException(error);
    }

    return output;
  }

  public abstract String getNodeId();

  public abstract Node toIdl();

  public SdkNode<OutputT> apply(String id, SdkTransform<OutputT> transform) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputBindings().isEmpty()
            ? Collections.singletonList(getNodeId())
            : Collections.emptyList();

    return builder.applyInternal(
        id, transform, upstreamNodeIds, /*metadata=*/ null, getOutputBindings());
  }
}
