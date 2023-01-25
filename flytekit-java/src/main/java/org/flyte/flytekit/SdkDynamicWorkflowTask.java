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
import org.flyte.api.v1.PartialTaskIdentifier;

public abstract class SdkDynamicWorkflowTask<InputT, OutputT>
    extends SdkTransform<InputT, OutputT> {

  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;

  @SuppressWarnings("PublicConstructorForAbstractClass")
  public SdkDynamicWorkflowTask(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
  }

  public String getType() {
    return "dynamic";
  }

  @Override
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(getName()).build();
    List<CompilerError> errors =
        Compiler.validateApply(nodeId, inputs, getInputType().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    return new SdkTaskNode<>(
        builder,
        nodeId,
        taskId,
        upstreamNodeIds,
        metadata,
        inputs,
        outputType.getVariableMap(),
        outputType.promiseFor(nodeId));
  }

  public abstract void run(SdkWorkflowBuilder builder, InputT input);

  /**
   * Number of retries. Retries will be consumed when dynamic workflow fails with a recoverable
   * error. The number of retries must be less than or equals to 10.
   *
   * @return number of retries
   */
  public int getRetries() {
    return 0;
  }
}
