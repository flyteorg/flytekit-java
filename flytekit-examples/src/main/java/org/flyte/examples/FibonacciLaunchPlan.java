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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkLaunchPlan;
import org.flyte.flytekit.SdkLaunchPlanRegistry;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SimpleSdkLaunchPlanRegistry;

@AutoService(SdkLaunchPlanRegistry.class)
public class FibonacciLaunchPlan extends SimpleSdkLaunchPlanRegistry {

  public FibonacciLaunchPlan() {
    // Register a default launch plan for all workflow
    registerDefaultLaunchPlans();
    // Add a launch plan with SdkType for inputs
    registerLaunchPlan(
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflowLaunchPlan")
            .withFixedInputs(SdkTypes.autoValue(Input.class), Input.create(0, 1)));

    // Launch plan specifying inputs directly
    registerLaunchPlan(
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflowLaunchPlan2")
            .withFixedInput("fib0", 0L)
            .withFixedInput("fib1", 1L));
  }

  @AutoValue
  abstract static class Input {
    abstract long fib0();

    abstract long fib1();

    public static Input create(long fib0, long fib1) {
      return new AutoValue_FibonacciLaunchPlan_Input(fib0, fib1);
    }
  }
}
