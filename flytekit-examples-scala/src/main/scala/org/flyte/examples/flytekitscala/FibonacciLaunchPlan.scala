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
package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkLaunchPlan, SimpleSdkLaunchPlanRegistry}
import org.flyte.flytekitscala.SdkScalaType

case class FibonacciLaunchPlanInput(fib0: Long, fib1: Long)

class FibonacciLaunchPlan extends SimpleSdkLaunchPlanRegistry {
  // Register default launch plans for all workflows
  registerDefaultLaunchPlans()

  // Register launch plan with fixed inputs using SdkType
  registerLaunchPlan(
    SdkLaunchPlan
      .of(new FibonacciWorkflow)
      .withName("FibonacciWorkflowLaunchPlan")
      .withFixedInputs(
        SdkScalaType[FibonacciLaunchPlanInput],
        FibonacciLaunchPlanInput(0, 1)
      )
  )

  // Register launch plan with fixed inputs specified directly
  registerLaunchPlan(
    SdkLaunchPlan
      .of(new FibonacciWorkflow)
      .withName("FibonacciWorkflowLaunchPlan2")
      .withFixedInput("fib0", 0L)
      .withFixedInput("fib1", 1L)
  )

  // Register launch plan with default inputs specified directly
  registerLaunchPlan(
    SdkLaunchPlan
      .of(new FibonacciWorkflow)
      .withName("FibonacciWorkflowLaunchPlan3")
      .withDefaultInput("fib0", 0L)
      .withDefaultInput("fib1", 1L)
  )
}
