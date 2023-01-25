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

import org.flyte.flytekit.{SdkBindingData, SdkRemoteLaunchPlan}
import org.flyte.flytekitscala.SdkScalaType

case class RemoteLaunchPlanInput(
    fib0: SdkBindingData[Long],
    fib1: SdkBindingData[Long]
)

case class RemoteLaunchPlanOutput(fib5: SdkBindingData[Long])

class RemoteLaunchPlan {

  /** Note that there is no SdkRemoteWorkflow. You need to register a launch
    * plan for the workflow to achieve using a remote workflow.
    */
  def create
      : SdkRemoteLaunchPlan[RemoteLaunchPlanInput, RemoteLaunchPlanOutput] = {
    SdkRemoteLaunchPlan.create(
      /* domain= */ "development", /* project= */ "flytesnacks",
      /* name= */ "FibonacciWorkflowLaunchPlan",
      SdkScalaType[RemoteLaunchPlanInput],
      SdkScalaType[RemoteLaunchPlanOutput]
    )
  }
}
