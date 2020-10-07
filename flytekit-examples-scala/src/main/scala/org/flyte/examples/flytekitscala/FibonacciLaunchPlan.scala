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

import java.util

import org.flyte.flytekit.{SdkLaunchPlan, SdkLaunchPlanRegistry}
import org.flyte.flytekitscala.SdkScalaType

import scala.collection.JavaConverters.seqAsJavaListConverter

case class FibonacciLaunchPlanInput(fib0: Long, fib1: Long)

class FibonacciLaunchPlan extends SdkLaunchPlanRegistry {
  override def getLaunchPlans: util.List[SdkLaunchPlan] =
    List(
      SdkLaunchPlan
        .of(new FibonacciWorkflow)
        .withFixedInputs(
          SdkScalaType[FibonacciLaunchPlanInput],
          FibonacciLaunchPlanInput(0, 1)
        ),
      SdkLaunchPlan
        .of(new FibonacciWorkflow)
        .withName("FibonacciWorkflow.alternative-name")
        .withFixedInput("fib0", 0L)
        .withFixedInput("fib1", 1L)
    ).asJava
}
