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

import org.flyte.flytekit.{
  SdkBindingData,
  SdkDynamicWorkflowTask,
  SdkWorkflowBuilder
}
import org.flyte.flytekitscala.SdkScalaType

import scala.annotation.tailrec

case class DynamicFibonacciWorkflowTaskInput(n: SdkBindingData[Long])
case class DynamicFibonacciWorkflowTaskOutput(output: SdkBindingData[Long])

class DynamicFibonacciWorkflowTask
    extends SdkDynamicWorkflowTask[
      DynamicFibonacciWorkflowTaskInput,
      DynamicFibonacciWorkflowTaskOutput
    ](
      SdkScalaType[DynamicFibonacciWorkflowTaskInput],
      SdkScalaType[DynamicFibonacciWorkflowTaskOutput]
    ) {

  override def run(
      builder: SdkWorkflowBuilder,
      input: DynamicFibonacciWorkflowTaskInput
  ): Unit = {

    @tailrec
    def fib(
        n: java.lang.Long,
        value: SdkBindingData[java.lang.Long],
        prev: SdkBindingData[java.lang.Long]
    ): SdkBindingData[java.lang.Long] = {
      if (n == input.n.get()) value
      else
        fib(
          n + 1,
          builder(s"fib-${n + 1}", SumTask(value, prev)).getOutputs.c,
          value
        )
    }

    require(input.n.get() > 0, "n < 0")
    val value = if (input.n.get() == 0) {
      SdkBindingData.ofInteger(0)
    } else {
      fib(1, SdkBindingData.ofInteger(1), SdkBindingData.ofInteger(0))
    }
    builder.output("output", value)
  }

}
