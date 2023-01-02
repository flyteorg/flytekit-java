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

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkRemoteLaunchPlan;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

// Normally the AutoService annotation would be uncommented, but the integration
// test would try to register this workflow, and it would expect the referenced
// launchplan to be registered already.
// The order that we register objects in jflyte is: task, workflows and launchplans
// @AutoService(SdkWorkflow.class)
public class RemoteLaunchPlanExample extends SdkWorkflow<NopOutputTransformer> {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData fib0 = builder.inputOfInteger("fib0");
    SdkBindingData fib1 = builder.inputOfInteger("fib1");
    builder.apply("remote-launch-plan", create().withInput("fib0", fib0).withInput("fib1", fib1));
  }

  public static SdkRemoteLaunchPlan<Input, Void, NopOutputTransformer> create() {
    return SdkRemoteLaunchPlan.create(
        /* domain= */ "development",
        /* project= */ "flytesnacks",
        /* name= */ "FibonacciWorkflowLaunchPlan",
        JacksonSdkType.of(Input.class),
        SdkTypes.nulls());
  }

  @AutoValue
  public abstract static class Input {
    abstract long fib0();

    abstract long fib1();

    public static Input create(long fib0, long fib1) {
      return new AutoValue_RemoteLaunchPlanExample_Input(fib0, fib1);
    }
  }
}
