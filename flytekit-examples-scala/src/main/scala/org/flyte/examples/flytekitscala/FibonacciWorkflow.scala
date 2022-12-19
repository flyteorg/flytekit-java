/*
 * Copyright 2021 Flyte Authors.
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
package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{NopNamedOutput, SdkWorkflow, SdkWorkflowBuilder}

class FibonacciWorkflow extends SdkWorkflow[NopNamedOutput] {

  def expand(builder: SdkWorkflowBuilder): Unit = {
    val fib0 = builder.inputOfInteger("fib0", "Value for Fib0")
    val fib1 = builder.inputOfInteger("fib1", "Value for Fib1")

    val fib2 = builder.apply("fib-2", SumTask(fib0, fib1)).getOutput("c")
    val fib3 = builder.apply("fib-3", SumTask(fib1, fib2)).getOutput("c")
    val fib4 = builder.apply("fib-4", SumTask(fib2, fib3)).getOutput("c")
    val fib5 = builder.apply("fib-5", SumTask(fib3, fib4)).getOutput("c")

    builder.output("fib5", fib5, "Value for Fib5")
  }
}
