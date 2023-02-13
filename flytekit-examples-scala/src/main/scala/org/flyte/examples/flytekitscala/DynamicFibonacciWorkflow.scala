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

import org.flyte.flytekit.SdkBindingData
import org.flyte.flytekitscala.{
  SdkScalaType,
  SdkScalaWorkflow,
  SdkScalaWorkflowBuilder
}

case class DynamicFibonacciWorkflowInput(n: SdkBindingData[Long])
case class DynamicFibonacciWorkflowOutput(output: SdkBindingData[Long])
class DynamicFibonacciWorkflow
    extends SdkScalaWorkflow[
      DynamicFibonacciWorkflowInput,
      DynamicFibonacciWorkflowOutput
    ](
      SdkScalaType[DynamicFibonacciWorkflowInput],
      SdkScalaType[DynamicFibonacciWorkflowOutput]
    ) {

  override def expand(
      builder: SdkScalaWorkflowBuilder,
      input: DynamicFibonacciWorkflowInput
  ): DynamicFibonacciWorkflowOutput = {

    val fibonacci = builder.apply(
      "fibonacci",
      new DynamicFibonacciWorkflowTask(),
      DynamicFibonacciWorkflowTaskInput(input.n)
    )

    DynamicFibonacciWorkflowOutput(fibonacci.getOutputs.output)
  }

}
