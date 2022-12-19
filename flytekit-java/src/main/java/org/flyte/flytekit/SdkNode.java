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
public abstract class SdkNode<NamedOutputT extends NamedOutput> {

  protected final SdkWorkflowBuilder builder;

  private final Class<NamedOutputT> namedOutputClass;

  private NamedOutputT namedOutput;

  protected SdkNode(SdkWorkflowBuilder builder) {
    this(builder, null);
  }

  protected SdkNode(SdkWorkflowBuilder builder, Class<NamedOutputT> namedOutputClass) {
    this.builder = builder;
    this.namedOutputClass = namedOutputClass;
  }

  public NamedOutputT getNamedOutput() {
    if (namedOutput == null) {
      if (namedOutputClass == null) {
        String message =
            String.format(
                "Try to use a named output without specific a typed output class from node: %s",
                getNodeId());
        CompilerError error =
            CompilerError.create(
                CompilerError.Kind.USED_NAMED_OUTPUT_WITHOUT_SPECIFIC_CLASS,
                /* nodeId= */ getNodeId(),
                /* message= */ message);

        throw new CompilerException(error);
      } else {
        initializeNamedOutput();
      }
    }

    return namedOutput;
  }

  private void initializeNamedOutput() {
    try {
      Constructor<? extends NamedOutputT> ctor = namedOutputClass.getConstructor(Map.class);
      this.namedOutput = ctor.newInstance(getOutputs());
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

  public SdkNode<NamedOutputT> apply(String id, SdkTransform<NamedOutputT> transform) {
    return apply(id, transform, null);
  }

  public SdkNode<NamedOutputT> apply(String id, SdkTransform<NamedOutputT> transform, Class<NamedOutputT> namedOutputClass) {
    // if there are no outputs, explicitly specify dependency to preserve execution order
    List<String> upstreamNodeIds =
        getOutputs().isEmpty() ? Collections.singletonList(getNodeId()) : Collections.emptyList();

    return builder.applyInternal(
        id, transform, upstreamNodeIds, /*metadata=*/ null, getOutputs(), namedOutputClass);
  }
}
