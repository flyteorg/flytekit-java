/*
 * Copyright 2020-2023 Flyte Authors.
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
package org.flyte.flytekit.testing;

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TypedInterface;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkType;

/** {@link RunnableTask} that can fix output for specific input. */
class TestingRunnableTask<InputT, OutputT>
    extends TestingRunnableNode<
        PartialTaskIdentifier, InputT, OutputT, TestingRunnableTask<InputT, OutputT>>
    implements RunnableTask {
  private TestingRunnableTask(
      PartialTaskIdentifier taskId,
      SdkType<InputT> inputType,
      SdkType<OutputT> outputType,
      Function<InputT, OutputT> runFn,
      boolean runFnProvided,
      Map<InputT, OutputT> fixedOutputs) {
    super(
        taskId,
        inputType,
        outputType,
        runFn,
        runFnProvided,
        fixedOutputs,
        TestingRunnableTask::new,
        "task",
        "SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask");
  }

  static <InputT, OutputT> TestingRunnableTask<InputT, OutputT> create(
      SdkRunnableTask<InputT, OutputT> task) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(task.getName()).build();

    return new TestingRunnableTask<>(
        taskId, task.getInputType(), task.getOutputType(), task::run, false, emptyMap());
  }

  static <InputT, OutputT> TestingRunnableTask<InputT, OutputT> create(
      String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(name).build();

    return new TestingRunnableTask<>(
        taskId, inputType, outputType, /* runFn= */ null, false, emptyMap());
  }

  @Override
  public TypedInterface getInterface() {
    return TypedInterface.builder()
        .inputs(inputType.getVariableMap())
        .outputs(outputType.getVariableMap())
        .build();
  }

  @Override
  public RetryStrategy getRetries() {
    // no retries in testing
    return RetryStrategy.builder().retries(1).build();
  }
}
