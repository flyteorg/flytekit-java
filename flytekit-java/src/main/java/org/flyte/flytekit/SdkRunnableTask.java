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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.Variable;

/** Building block for tasks that execute Java code. */
public abstract class SdkRunnableTask<InputT, OutputT> extends SdkTransform<InputT, OutputT>
    implements Serializable {

  private static final long serialVersionUID = 42L;

  private final transient SdkType<InputT> inputType;
  private final transient SdkType<OutputT> outputType;

  @SuppressWarnings("PublicConstructorForAbstractClass")
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

  public String getType() {
    return "java-task";
  }

  @Override
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  public SdkStruct getCustom() {
    return SdkStruct.empty();
  }

  public SdkResources getResources() {
    return SdkResources.empty();
  }

  /**
   * Number of retries. Retries will be consumed when the task fails with a recoverable error. The
   * number of retries must be less than or equals to 10.
   *
   * @return number of retries
   */
  public int getRetries() {
    return 0;
  }

  /**
   * Indicates whether the system should attempt to look up this task's output to avoid duplication
   * of work.
   */
  public boolean isCached() {
    return false;
  }

  /** Indicates a logical version to apply to this task for the purpose of cache. */
  public String getCacheVersion() {
    return null;
  }

  /**
   * Indicates whether the system should attempt to execute cached instances in serial to avoid
   * duplicate work.
   */
  public boolean isCacheSerializable() {
    return false;
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

    Map<String, Variable> variableMap = outputType.getVariableMap();
    OutputT output = outputType.promiseFor(nodeId);
    return new SdkTaskNode<>(
        builder, nodeId, taskId, upstreamNodeIds, metadata, inputs, variableMap, output);
  }

  public abstract OutputT run(InputT input);
}
