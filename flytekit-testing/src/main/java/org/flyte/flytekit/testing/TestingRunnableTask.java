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
package org.flyte.flytekit.testing;

import static java.util.Collections.emptyMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TypedInterface;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkType;

/** {@link RunnableTask} that can fix output for specific input. */
class TestingRunnableTask<InputT, OutputT> implements RunnableTask {

  private final PartialTaskIdentifier taskId;
  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;

  // @Nullable
  private final Function<InputT, OutputT> runFn;

  private final Map<InputT, OutputT> fixedOutputs;

  private TestingRunnableTask(
      PartialTaskIdentifier taskId,
      SdkType<InputT> inputType,
      SdkType<OutputT> outputType,
      Function<InputT, OutputT> runFn,
      Map<InputT, OutputT> fixedOutputs) {
    this.taskId = taskId;
    this.inputType = inputType;
    this.outputType = outputType;
    this.runFn = runFn;
    this.fixedOutputs = fixedOutputs;
  }

  static <InputT, OutputT> TestingRunnableTask<InputT, OutputT> create(
      SdkRunnableTask<InputT, OutputT> task) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(task.getName()).build();

    return new TestingRunnableTask<>(
        taskId, task.getInputType(), task.getOutputType(), task::run, emptyMap());
  }

  static <InputT, OutputT> TestingRunnableTask<InputT, OutputT> create(
      String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    PartialTaskIdentifier taskId = PartialTaskIdentifier.builder().name(name).build();

    return new TestingRunnableTask<>(taskId, inputType, outputType, /* runFn= */ null, emptyMap());
  }

  @Override
  public String getName() {
    return taskId.name();
  }

  @Override
  public TypedInterface getInterface() {
    return TypedInterface.builder()
        .inputs(inputType.getVariableMap())
        .outputs(outputType.getVariableMap())
        .build();
  }

  @Override
  public Map<String, Literal> run(Map<String, Literal> inputs) {
    InputT input = inputType.fromLiteralMap(inputs);
    OutputT output = fixedOutputs.get(input);

    if (output != null) {
      return outputType.toLiteralMap(output);
    }

    if (runFn == null) {
      String message =
          String.format(
              "Can't find input %s for remote task [%s] across known task inputs, "
                  + "use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask",
              input, getName());

      throw new IllegalArgumentException(message);
    }

    return outputType.toLiteralMap(runFn.apply(input));
  }

  @Override
  public RetryStrategy getRetries() {
    // no retries in testing
    return RetryStrategy.builder().retries(1).build();
  }

  public TestingRunnableTask<InputT, OutputT> withFixedOutput(InputT input, OutputT output) {
    Map<InputT, OutputT> newFixedOutputs = new HashMap<>(fixedOutputs);
    newFixedOutputs.put(input, output);

    return new TestingRunnableTask<>(taskId, inputType, outputType, runFn, newFixedOutputs);
  }

  public TestingRunnableTask<InputT, OutputT> withRunFn(Function<InputT, OutputT> runFn) {
    return new TestingRunnableTask<>(taskId, inputType, outputType, runFn, fixedOutputs);
  }

  @Override
  public String getType() {
    return "java-task";
  }

  @Override
  public Map<String, Literal> getCustom() {
    return Collections.emptyMap();
  }
}
