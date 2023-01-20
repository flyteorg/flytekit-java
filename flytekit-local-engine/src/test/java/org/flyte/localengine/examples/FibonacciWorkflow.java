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
package org.flyte.localengine.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow<FibonacciWorkflow.Output> {

  public FibonacciWorkflow() {
    super(JacksonSdkType.of(FibonacciWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> fib0 = builder.inputOfInteger("fib0");
    SdkBindingData<Long> fib1 = builder.inputOfInteger("fib1");

    SdkBindingData<Long> fib2 =
        builder
            .apply("fib-2", new SumTask().withInput("a", fib0).withInput("b", fib1))
            .getOutputs()
            .o();

    SdkBindingData<Long> fib3 =
        builder
            .apply("fib-3", new SumTask().withInput("a", fib1).withInput("b", fib2))
            .getOutputs()
            .o();

    SdkBindingData<Long> fib4 =
        builder
            .apply("fib-4", new SumTask().withInput("a", fib2).withInput("b", fib3))
            .getOutputs()
            .o();

    SdkBindingData<Long> fib5 =
        builder
            .apply("fib-5", new SumTask().withInput("a", fib3).withInput("b", fib4))
            .getOutputs()
            .o();

    builder.output("fib4", fib4);
    builder.output("fib5", fib5);
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<Long> fib4();

    public abstract SdkBindingData<Long> fib5();
  }
}
