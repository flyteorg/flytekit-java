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

import java.util.Map;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

/** {@link SdkWorkflow} that can fix output for specific input. */
class TestingWorkflow<InputT, OutputT> extends SdkWorkflow<InputT, OutputT> {

  private final Map<InputT, OutputT> outputs;

  TestingWorkflow(
      SdkType<InputT> inputType, SdkType<OutputT> outputType, Map<InputT, OutputT> outputs) {
    super(inputType, outputType);
    this.outputs = outputs;
  }

  @Override
  public OutputT expand(SdkWorkflowBuilder builder, InputT input) {
    return builder
        .apply(new TestingSdkRunnableTask<>(getInputType(), getOutputType(), outputs), input)
        .getOutputs();
  }

  public static class TestingSdkRunnableTask<InputT, OutputT>
      extends SdkRunnableTask<InputT, OutputT> {
    private static final long serialVersionUID = 6106269076155338045L;

    private final Map<InputT, OutputT> outputs;

    public TestingSdkRunnableTask(
        SdkType<InputT> inputType, SdkType<OutputT> outputType, Map<InputT, OutputT> outputs) {
      super(inputType, outputType);
      this.outputs = outputs;
    }

    @Override
    public OutputT run(InputT input) {
      return outputs.get(input);
    }
  }
}
