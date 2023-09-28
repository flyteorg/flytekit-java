/*
 * Copyright 2020-2023 Flyte Authors
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
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class UberWorkflow extends SdkWorkflow<UberWorkflow.Input, SumWorkflow.Output> {

  @AutoValue
  public abstract static class Input {

    public abstract SdkBindingData<Long> a();

    public abstract SdkBindingData<Long> b();

    public abstract SdkBindingData<Long> c();

    public abstract SdkBindingData<Long> d();

    public static UberWorkflow.Input create(
        SdkBindingData<Long> a,
        SdkBindingData<Long> b,
        SdkBindingData<Long> c,
        SdkBindingData<Long> d) {
      return new AutoValue_UberWorkflow_Input(a, b, c, d);
    }
  }

  public UberWorkflow() {
    super(JacksonSdkType.of(UberWorkflow.Input.class), JacksonSdkType.of(SumWorkflow.Output.class));
  }

  @Override
  public SumWorkflow.Output expand(SdkWorkflowBuilder builder, Input input) {
    SdkBindingData<Long> a = input.a();
    SdkBindingData<Long> b = input.b();
    SdkBindingData<Long> c = input.c();
    SdkBindingData<Long> d = input.d();
    SdkBindingData<Long> ab =
        builder
            .apply("sub-1", new SumWorkflow(), SumWorkflow.Input.create(a, b))
            .getOutputs()
            .result();
    SdkBindingData<Long> abc =
        builder
            .apply("sub-2", new SumWorkflow(), SumWorkflow.Input.create(ab, c))
            .getOutputs()
            .result();
    SdkBindingData<Long> abcd =
        builder.apply("post-sum", new SumTask(), SumTask.SumInput.create(abc, d)).getOutputs();
    SdkBindingData<Long> result =
        builder
            .apply(
                "fibonacci",
                new DynamicFibonacciWorkflowTask(),
                DynamicFibonacciWorkflowTask.Input.create(abcd))
            .getOutputs()
            .output();
    return SumWorkflow.Output.create(result);
  }
}
