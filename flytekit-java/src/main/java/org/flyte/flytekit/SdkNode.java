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
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Node;

/** Represent a node in the workflow DAG. */
public abstract class SdkNode<OutputTransformerT extends OutputTransformer> {

  protected final SdkWorkflowBuilder builder;

  private final Class<OutputTransformerT> outputTransformerClass;

  private OutputTransformerT outputTransformer;

  protected SdkNode(SdkWorkflowBuilder builder) {
    this(builder, null);
  }

  protected SdkNode(SdkWorkflowBuilder builder, Class<OutputTransformerT> outputTransformerClass) {
    this.builder = builder;
    this.outputTransformerClass = outputTransformerClass;
  }

  public OutputTransformerT getOutputTransformer() {
    if (outputTransformer == null) {
      if (outputTransformerClass == null) {
        String message =
            String.format(
                "Try to use a output transformer without specific a output transformer class from node: %s",
                getNodeId());
        CompilerError error =
            CompilerError.create(
                CompilerError.Kind.USED_OUTPUT_TRANSFORMER_WITHOUT_SPECIFIC_CLASS,
                /* nodeId= */ getNodeId(),
                /* message= */ message);

        throw new CompilerException(error);
      } else {
        initializeOutputTransformer();
      }
    }

    return outputTransformer;
  }

  private void initializeOutputTransformer() {
    try {
      Constructor<? extends OutputTransformerT> ctor = outputTransformerClass.getConstructor(Map.class);
      this.outputTransformer = ctor.newInstance(getOutputs());
    } catch (IllegalAccessException
        | InstantiationException
        | NoSuchMethodException
        | InvocationTargetException ex) {
      ex.printStackTrace();
    }
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

  public SdkNode<OutputTransformerT> apply(String id, SdkTransform<OutputTransformerT> transform) {
    return apply(id, transform, null);
  }

  public SdkNode<OutputTransformerT> apply(String id, SdkTransform<OutputTransformerT> transform, Class<OutputTransformerT> outputTransformerClass) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputs().isEmpty() ? Collections.singletonList(getNodeId()) : Collections.emptyList();

    return builder.applyInternal(
        id, transform, upstreamNodeIds, /*metadata=*/ null, getOutputs(), outputTransformerClass);
  }
}
