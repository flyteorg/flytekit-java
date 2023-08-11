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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.Description;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class SumWorkflow extends SdkWorkflow<SumWorkflow.Input, SumWorkflow.Output> {

  public SumWorkflow() {
    super(JacksonSdkType.of(SumWorkflow.Input.class), JacksonSdkType.of(SumWorkflow.Output.class));
  }

  @Override
  public Output expand(SdkWorkflowBuilder builder, Input input) {
    SdkBindingData<Long> result =
        builder
            .apply("sum", new SumTask(), SumTask.SumInput.create(input.left(), input.right()))
            .getOutputs();
    return Output.create(result);
  }

  // Used in testing to mock this workflow
  @AutoValue
  public abstract static class Input {
    @Description("First operand")
    abstract SdkBindingData<Long> left();

    @Description("Second operand")
    abstract SdkBindingData<Long> right();

    public static Input create(SdkBindingData<Long> left, SdkBindingData<Long> right) {
      return new AutoValue_SumWorkflow_Input(left, right);
    }
  }

  @AutoValue
  public abstract static class Output {
    @Description("Summed results")
    abstract SdkBindingData<Long> result();

    public static Output create(SdkBindingData<Long> result) {
      return new AutoValue_SumWorkflow_Output(result);
    }
  }
}
