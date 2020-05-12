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
package org.flyte.jflyte.examples;

import static org.flyte.flytekit.SdkWorkflowBuilder.literalOfInteger;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class FibonacciWorkflow extends SdkWorkflow {

  @Override
  @SuppressWarnings("UnusedVariable")
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData fib0 = literalOfInteger(0L);

    SdkBindingData fib1 = literalOfInteger(1L);

    SdkBindingData fib2 =
        builder.mapOf("a", fib0, "b", fib1).apply("fib-2", new SumTask()).getOutput("c");

    SdkBindingData fib3 =
        builder.mapOf("a", fib1, "b", fib2).apply("fib-3", new SumTask()).getOutput("c");

    SdkBindingData fib4 =
        builder.mapOf("a", fib2, "b", fib3).apply("fib-4", new SumTask()).getOutput("c");

    // fib5 =
    builder.mapOf("a", fib3, "b", fib4).apply("fib-5", new SumTask()).getOutput("c");
  }
}
