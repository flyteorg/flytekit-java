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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData fib0 = builder.inputOfInteger("fib0", "Value for Fib0");
    SdkBindingData fib1 = builder.inputOfString("fib1", "Value for Fib1");

    SdkBindingData fib2 =
        builder.apply("fib-2", SumTask.of(fib0, fib1).withInput("d", fib0)).getOutput("c");
    SdkBindingData fib3 = builder.apply("fib-3", SumTask.of(fib1, fib2)).getOutput("c");
    SdkBindingData fib4 = builder.apply("fib-4", SumTask.of(fib2, fib3)).getOutput("c");
    SdkBindingData fib5 = builder.apply("fib-5", SumTask.of(fib3, fib4)).getOutput("c");

    builder.output("fib5", fib5, "Value for Fib5");
  }
}
