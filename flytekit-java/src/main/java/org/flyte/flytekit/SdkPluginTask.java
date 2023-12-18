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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;

/**
 * A task that is handled by a Flyte backend plugin instead of run as a container. Note that a
 * plugin task template does not have a container defined, neither all the jars captured in
 * classpath, so if this is a requirement, one should use SdkRunnableTask overriding run method to
 * simply return null.
 */
public abstract class SdkPluginTask<InputT, OutputT> extends SdkTransform<InputT, OutputT> {

  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;

  /**
   * Called by subclasses passing the {@link SdkType}s for inputs and outputs.
   *
   * @param inputType type for inputs.
   * @param outputType type for outputs.
   */
  public SdkPluginTask(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
  }

  public abstract String getType();

  @Override
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  /** Specifies custom data that can be read by the backend plugin. */
  public SdkStruct getCustom() {
    return SdkStruct.empty();
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
  SdkNode<OutputT> apply(
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
        builder, nodeId, taskId, upstreamNodeIds, metadata, inputs, outputType);
  }
}
