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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.PartialTaskIdentifier;

/** Building block for tasks that execute Java code. */
public abstract class SdkRunnableTask<InputT, OutputT> extends SdkTransform
    implements Serializable {

  static final long serialVersionUID = 42L;

  private final transient SdkType<InputT> inputType;
  private final transient SdkType<OutputT> outputType;

  public SdkRunnableTask(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
  }

  // constructor used for serialization
  //
  // we want to make class serializable because big data processing frameworks
  // tend to capture outer classes into closures, and serialize them
  private SdkRunnableTask() {
    this.inputType = null;
    this.outputType = null;
  }

  public String getName() {
    return getClass().getName();
  }

  public SdkType<InputT> getInputType() {
    return inputType;
  }

  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> inputs) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(getName()).build();
    List<CompilerError> errors =
        Compiler.validateApply(nodeId, inputs, getInputType().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    return new SdkTaskNode(
        builder, nodeId, taskId, upstreamNodeIds, inputs, outputType.getVariableMap());
  }

  public abstract OutputT run(InputT input);
}
