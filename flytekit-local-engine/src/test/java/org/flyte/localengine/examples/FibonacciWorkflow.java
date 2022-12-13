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
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData fib0 = builder.inputOfInteger("fib0");
    SdkBindingData fib1 = builder.inputOfInteger("fib1");

    SdkNode<?> fib2 =
        builder.apply("fib-2", new SumTask().withInput("a", fib0).withInput("b", fib1));

    SdkNode<?> fib3 =
        builder.apply(
            "fib-3", new SumTask().withInput("a", fib1).withInput("b", fib2.getOutput("c")));

    SdkNode<?> fib4 =
        builder.apply(
            "fib-4",
            new SumTask().withInput("a", fib2.getOutput("c")).withInput("b", fib3.getOutput("c")));

    SdkNode<?> fib5 =
        builder.apply(
            "fib-5",
            new SumTask().withInput("a", fib3.getOutput("c")).withInput("b", fib4.getOutput("c")));

    builder.output("fib4", fib4.getOutput("c"));
    builder.output("fib5", fib5.getOutput("c"));
  }
}
