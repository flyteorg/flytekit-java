/*
 * Copyright 2023 Flyte Authors.
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;

public abstract class SdkMapTask<InputT, OutputT> extends SdkRunnableTask<InputT, OutputT>
    implements Serializable {

  /**
   * Called by subclasses passing the {@link SdkType}s for inputs and outputs.
   *
   * @param inputType type for inputs.
   * @param outputType type for outputs.
   */
  public SdkMapTask(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    super(inputType, outputType);
  }

  @Override
  public String getType() {
    return "container_array";
  }

  @Override
  public String getName() {
    // TODO add something random
    return getClass().getName() + "MapTask";
  }

  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {
    // TODO add checks for the inputs and outputs
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(getName()).build();
    List<CompilerError> errors =
        Compiler.validateApply(nodeId, inputs, getInputType().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    return new SdkTaskNode<>(
        builder, nodeId, taskId, upstreamNodeIds, metadata, inputs, this.getOutputType());
  }
}
