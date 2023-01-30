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

import org.flyte.flytekit.{SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekitscala.{SdkBindingData, SdkScalaType}

class WorkflowWithRemoteLaunchPlan
    extends SdkWorkflow[RemoteLaunchPlanInput, RemoteLaunchPlanOutput](
      SdkScalaType[RemoteLaunchPlanInput],
      SdkScalaType[RemoteLaunchPlanOutput]
    ) {

  override def expand(builder: SdkWorkflowBuilder, input: RemoteLaunchPlanInput): RemoteLaunchPlanOutput = {
    val fib0 = SdkBindingData.ofInteger(0L)
    val fib1 = SdkBindingData.ofInteger(1L)

    val fib5 = builder
      .apply(
        new RemoteLaunchPlan().create,
        RemoteLaunchPlanInput(fib0, fib1)
      )
      .getOutputs
      .fib5
    RemoteLaunchPlanOutput(fib5)
  }
}
