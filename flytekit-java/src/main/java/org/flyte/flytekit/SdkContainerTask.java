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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;

/** Building block for tasks that execute arbitrary containers. */
public abstract class SdkContainerTask<InputT, OutputT> extends SdkTransform
    implements Serializable {

  private static final long serialVersionUID = 42L;

  private final transient SdkType<InputT> inputType;
  private final transient SdkType<OutputT> outputType;

  @SuppressWarnings("PublicConstructorForAbstractClass")
  public SdkContainerTask(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
  }

  // constructor used for serialization
  //
  // we want to make class serializable because big data processing frameworks
  // tend to capture outer classes into closures, and serialize them
  private SdkContainerTask() {
    this.inputType = null;
    this.outputType = null;
  }

  public String getType() {
    return "raw-container";
  }

  /** Specifies task name. */
  public String getName() {
    return getClass().getName();
  }

  /** Specifies task input type. */
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  /** Specifies task output type. */
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  /** Specifies custom container parameters. */
  public SdkStruct getCustom() {
    return SdkStruct.empty();
  }

  /** Specifies container resource requests. */
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

  @Override
  public <T extends TypedOutput> SdkNode<T> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs,
      Class<T> typedOutputClass) {
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
        typedOutputClass);
  }

  /** Specifies container image. */
  public abstract String getImage();

  /** Specifies command line arguments for container command. */
  public List<String> getArgs() {
    return emptyList();
  }

  /** Specifies container command. Example: {@code List.of("java", "-Xmx1G")}. */
  public List<String> getCommand() {
    return emptyList();
  }

  /** Specifies container environment variables. */
  public Map<String, String> getEnv() {
    return emptyMap();
  }

  /**
   * Indicates whether the system should attempt to lookup this task's output to avoid duplication
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
}
