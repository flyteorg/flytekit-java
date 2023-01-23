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
import java.util.List;
import java.util.Map;
import org.flyte.examples.BatchLookUpTask.Input;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class DynamicFibonacciWorkflow extends SdkWorkflow<DynamicFibonacciWorkflow.Input, DynamicFibonacciWorkflowTask.Output> {
  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<Long> n();

    public static DynamicFibonacciWorkflow.Input create(SdkBindingData<Long> n) {
      return new AutoValue_DynamicFibonacciWorkflow_Input(n);
    }
  }
  public DynamicFibonacciWorkflow() {
    super(JacksonSdkType.of(DynamicFibonacciWorkflow.Input.class), JacksonSdkType.of(DynamicFibonacciWorkflowTask.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> n = builder.inputOfInteger("n");

    SdkBindingData<Long> fibOutput =
        builder
            .apply("fibonacci", new DynamicFibonacciWorkflowTask(), DynamicFibonacciWorkflowTask.Input.create(n))
            .getOutputs()
            .output();

    builder.output("output", fibOutput);
  }
}
