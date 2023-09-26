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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkLiteralTypes;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.Description;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow<FibonacciWorkflow.Input, SdkBindingData<Long>> {

  public FibonacciWorkflow() {
    super(
        JacksonSdkType.of(FibonacciWorkflow.Input.class),
        SdkTypes.of(SdkLiteralTypes.integers(), "fib5", "Value for Fib5"));
  }

  @Override
  public SdkBindingData<Long> expand(SdkWorkflowBuilder builder, Input input) {

    SdkBindingData<Long> fib2 =
        builder
            .apply("fib-2", new SumTask(), SumTask.SumInput.create(input.fib1(), input.fib0()))
            .getOutputs();
    SdkBindingData<Long> fib3 =
        builder
            .apply("fib-3", new SumTask(), SumTask.SumInput.create(input.fib1(), fib2))
            .getOutputs();
    SdkBindingData<Long> fib4 =
        builder.apply("fib-4", new SumTask(), SumTask.SumInput.create(fib2, fib3)).getOutputs();
    SdkBindingData<Long> fib5 =
        builder.apply("fib-5", new SumTask(), SumTask.SumInput.create(fib3, fib4)).getOutputs();

    return fib5;
  }

  @AutoValue
  public abstract static class Input {

    @Description("Value for Fib0")
    public abstract SdkBindingData<Long> fib0();

    @Description("Value for Fib1")
    public abstract SdkBindingData<Long> fib1();

    public static FibonacciWorkflow.Input create(
        SdkBindingData<Long> fib0, SdkBindingData<Long> fib1) {
      return new AutoValue_FibonacciWorkflow_Input(fib0, fib1);
    }
  }
}
