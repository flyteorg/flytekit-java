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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Node;

/** Represent a node in the workflow DAG. */
public abstract class SdkNode<T extends TypedOutput> {

  protected final SdkWorkflowBuilder builder;
  T typedOutput;

  protected SdkNode(SdkWorkflowBuilder builder) {
    this(builder, null);
  }

  protected SdkNode(SdkWorkflowBuilder builder, Class<? extends T> typedOutputClass) {
    this.builder = builder;

    if (typedOutputClass != null) {
      try {
        Constructor<? extends T> ctor = typedOutputClass.getConstructor(Map.class);
        this.typedOutput = ctor.newInstance(getOutputs());
      } catch (Exception ex) {
        String message = String.format("error %s", getNodeId());
        CompilerError error =
            CompilerError.create(
                CompilerError.Kind.USED_TYPED_OUTPUT_WITHOUT_SUPPLIER,
                /* nodeId= */ getNodeId(),
                /* message= */ message);

        throw new CompilerException(error);
      }
    } else {
      this.typedOutput = null;
    }
  }

  public T getTypedOutput() {
    if (typedOutput == null) {
      String message = String.format("error %s", getNodeId());
      CompilerError error =
          CompilerError.create(
              CompilerError.Kind.USED_TYPED_OUTPUT_WITHOUT_SUPPLIER,
              /* nodeId= */ getNodeId(),
              /* message= */ message);

      throw new CompilerException(error);
    }

    return typedOutput;
  }

  public abstract Map<String, SdkBindingData> getOutputs();

  public SdkBindingData getOutput(String name) {
    SdkBindingData output = getOutputs().get(name);

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

  public SdkNode<T> apply(String id, SdkTransform transform) {
    return apply(id, transform, null);
  }

  public SdkNode<T> apply(String id, SdkTransform transform, Class<T> typedOutputClass) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputs().isEmpty() ? Collections.singletonList(getNodeId()) : Collections.emptyList();

    return builder.applyInternal(
        id, transform, upstreamNodeIds, /*metadata=*/ null, getOutputs(), typedOutputClass);
  }
}
