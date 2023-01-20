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
import com.google.errorprone.annotations.Var;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkDynamicWorkflowTask;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkDynamicWorkflowTask.class)
public class DynamicFibonacciWorkflowTask
    extends SdkDynamicWorkflowTask<
        DynamicFibonacciWorkflowTask.Input, DynamicFibonacciWorkflowTask.Output> {
  public DynamicFibonacciWorkflowTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @AutoValue
  abstract static class Input {
    public abstract SdkBindingData<Long> n();
  }

  @AutoValue
  abstract static class Output {
    public abstract SdkBindingData<Long> output();
  }

  @Override
  public void run(SdkWorkflowBuilder builder, Input input) {
    if (input.n().get() < 0) {
      throw new IllegalArgumentException("n < 0");
    } else if (input.n().get() == 0) {
      builder.output("output", SdkBindingData.ofInteger(0));
    } else {
      @Var SdkBindingData<Long> prev = SdkBindingData.ofInteger(0);
      @Var SdkBindingData<Long> value = SdkBindingData.ofInteger(1);
      for (int i = 2; i <= input.n().get(); i++) {
        SdkBindingData<Long> next =
            builder.apply("fib-" + i, SumTask.of(value, prev)).getOutputs().c();
        prev = value;
        value = next;
      }
      builder.output("output", value);
    }
  }
}
