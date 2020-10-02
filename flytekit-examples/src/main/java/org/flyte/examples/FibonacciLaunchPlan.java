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
import java.util.Arrays;
import java.util.List;
import org.flyte.flytekit.SdkLaunchPlan;
import org.flyte.flytekit.SdkLaunchPlanRegistry;
import org.flyte.flytekit.SdkTypes;

@AutoService(SdkLaunchPlanRegistry.class)
public class FibonacciLaunchPlan implements SdkLaunchPlanRegistry {

  @Override
  public List<SdkLaunchPlan> getLaunchPlans() {
    return Arrays.asList(
        // Using default naming and SdkType for inputs
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withFixedInputs(SdkTypes.autoValue(Input.class), Input.create(0, 1)),

        // With alternative name and specifying inputs directly
        SdkLaunchPlan.of(new FibonacciWorkflow())
            .withName("FibonacciWorkflow.alternative-name")
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
