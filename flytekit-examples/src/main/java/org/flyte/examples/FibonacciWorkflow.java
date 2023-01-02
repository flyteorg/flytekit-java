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
import java.util.concurrent.Future;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow<NopOutputTransformer> {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> fib0 = builder.inputOfInteger("fib0", "Value for Fib0");
    SdkBindingData<Long> fib1 = builder.inputOfInteger("fib1", "Value for Fib1");

    SdkBindingData<Long> fib2 = builder.apply("fib-2", SumTask.of(fib0, fib1)).getOutputs().c();
    SdkBindingData<Long> fib3 = builder.apply("fib-3", SumTask.of(fib1, fib2)).getOutputs().c();
    SdkBindingData<Long> fib4 = builder.apply("fib-4", SumTask.of(fib2, fib3)).getOutputs().c();
    SdkBindingData<Long> fib5 = builder.apply("fib-5", SumTask.of(fib3, fib4)).getOutputs().c();

    builder.output("fib5", fib5, "Value for Fib5");
  }
}
