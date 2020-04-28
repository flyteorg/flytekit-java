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
package org.flyte.examples.flytekitscala;

import com.google.auto.service.AutoService
import org.flyte.flytekit.{SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekit.SdkWorkflowBuilder.literalOf

@AutoService(Array(classOf[SdkWorkflow]))
object FibonacciWorkflow extends SdkWorkflow {

  def expand(builder: SdkWorkflowBuilder): Unit = {
    val fib0 = literalOf(0L);
    val fib1 = literalOf(1L);

    val fib2 =
      builder
        .mapOf("a", fib0, "b", fib1)
        .apply("fib-2", SumTask)
        .getOutput("c");

    val fib3 =
      builder
        .mapOf("a", fib1, "b", fib2)
        .apply("fib-3", SumTask)
        .getOutput("c");

    val fib4 =
      builder
        .mapOf("a", fib2, "b", fib3)
        .apply("fib-4", SumTask)
        .getOutput("c");

    // fib5 =
    builder.mapOf("a", fib3, "b", fib4).apply("fib-5", SumTask).getOutput("c");
  }
}
