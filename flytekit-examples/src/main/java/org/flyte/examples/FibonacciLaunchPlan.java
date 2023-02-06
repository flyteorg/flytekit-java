/*
 * Copyright 2021 Flyte Authors
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
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDatas;
import org.flyte.flytekit.SdkLaunchPlan;
import org.flyte.flytekit.SdkLaunchPlanRegistry;
import org.flyte.flytekit.SimpleSdkLaunchPlanRegistry;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkLaunchPlanRegistry.class)
public class FibonacciLaunchPlan extends SimpleSdkLaunchPlanRegistry {

  public FibonacciLaunchPlan() {
    // Register default launch plans for all workflows
    registerDefaultLaunchPlans();

    // Register launch plan with fixed inputs using SdkType
    registerLaunchPlan(
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflowLaunchPlan")
            .withFixedInputs(
                JacksonSdkType.of(Input.class),
                Input.create(SdkBindingDatas.ofInteger(0), SdkBindingDatas.ofInteger(1))));

    // Register launch plan with fixed inputs specified directly
    registerLaunchPlan(
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflowLaunchPlan2")
            .withFixedInput("fib0", 0L)
            .withFixedInput("fib1", 1L));

    // Register launch plan with default inputs specified directly
    registerLaunchPlan(
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflowLaunchPlan3")
            .withDefaultInput("fib0", 0L)
            .withDefaultInput("fib1", 1L));
  }

  @AutoValue
  abstract static class Input {
    abstract SdkBindingData<Long> fib0();

    abstract SdkBindingData<Long> fib1();

    public static Input create(SdkBindingData<Long> fib0, SdkBindingData<Long> fib1) {
      return new AutoValue_FibonacciLaunchPlan_Input(fib0, fib1);
    }
  }
}
