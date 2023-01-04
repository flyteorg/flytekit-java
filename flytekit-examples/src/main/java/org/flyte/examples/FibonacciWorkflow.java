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
import org.flyte.flytekit.SdkNode;
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
    SdkBindingData<Long> fib0 = builder.inputOfInteger("fib0", "Value for Fib0");
    SdkBindingData<Long> fib1 = builder.inputOfInteger("fib1", "Value for Fib1");

    SdkNode<SumTask.SumOutput> apply = builder.apply("fib-2", SumTask.of(fib0, fib1));
    SumTask.SumOutput outputs = apply.getOutputs();
    SdkBindingData<Long> fib2 = outputs.c();
    SdkBindingData<Long> fib3 = builder.apply("fib-3", SumTask.of(fib1, fib2)).getOutputs().c();
    SdkBindingData<Long> fib4 = builder.apply("fib-4", SumTask.of(fib2, fib3)).getOutputs().c();
    SdkBindingData<Long> fib5 = builder.apply("fib-5", SumTask.of(fib3, fib4)).getOutputs().c();

    builder.output("fib5", fib5, "Value for Fib5");
  }

    @AutoValue
    public abstract static class Output {
        public abstract SdkBindingData<Long> fib5();

        public static FibonacciWorkflow.Output create(Long fib5) {
            return new AutoValue_FibonacciWorkflow_Output(SdkBindingData.ofInteger(fib5));
        }
    }
}
