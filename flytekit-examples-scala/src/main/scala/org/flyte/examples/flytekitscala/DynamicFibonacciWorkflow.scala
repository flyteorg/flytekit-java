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

import org.flyte.flytekit.{SdkBindingData, SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekitscala.SdkScalaType

case class DynamicFibonacciWorkflowOutput(output: SdkBindingData[Long])
class DynamicFibonacciWorkflow
    extends SdkWorkflow[DynamicFibonacciWorkflowOutput](
      SdkScalaType[DynamicFibonacciWorkflowOutput]
    ) {

  override def expand(builder: SdkWorkflowBuilder): Unit = {
    val n = builder.inputOfInteger("n")

    val fibonacci = builder.apply(
      "fibonacci",
      new DynamicFibonacciWorkflowTask().withInput("n", n)
    )

    builder.output("output", fibonacci.getOutputs.output)
  }

}
