/*
 * Copyright 2021-2023 Flyte Authors.
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

import static org.flyte.examples.FlyteEnvironment.DOMAIN;
import static org.flyte.examples.FlyteEnvironment.PROJECT;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Var;
import org.flyte.examples.SumTask.SumInput;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkDynamicWorkflowTask;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkDynamicWorkflowTask.class)
public class DynamicFibonacciWorkflowTask
    extends SdkDynamicWorkflowTask<
        DynamicFibonacciWorkflowTask.Input, DynamicFibonacciWorkflowTask.Output> {
  public DynamicFibonacciWorkflowTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @AutoValue
  abstract static class Input {
    public abstract SdkBindingData<Long> n();

    public static DynamicFibonacciWorkflowTask.Input create(SdkBindingData<Long> n) {
      return new AutoValue_DynamicFibonacciWorkflowTask_Input(n);
    }
  }

  @AutoValue
  abstract static class Output {
    public abstract SdkBindingData<Long> output();

    public static DynamicFibonacciWorkflowTask.Output create(SdkBindingData<Long> output) {
      return new AutoValue_DynamicFibonacciWorkflowTask_Output(output);
    }
  }

  @Override
  public Output run(SdkWorkflowBuilder builder, Input input) {
    if (input.n().get() < 0) {
      throw new IllegalArgumentException("n < 0");
    } else if (input.n().get() == 0) {
      return Output.create(SdkBindingDataFactory.of(0));
    } else {
      SdkNode<Void> hello =
          builder.apply(
              "hello",
              SdkRemoteTask.create(
                  DOMAIN,
                  PROJECT,
                  HelloWorldTask.class.getName(),
                  SdkTypes.nulls(),
                  SdkTypes.nulls()));
      @Var SdkBindingData<Long> prev = SdkBindingDataFactory.of(0);
      @Var SdkBindingData<Long> value = SdkBindingDataFactory.of(1);
      for (int i = 2; i <= input.n().get(); i++) {
        SdkBindingData<Long> next =
            builder
                .apply(
                    "fib-" + i, new SumTask().withUpstreamNode(hello), SumInput.create(value, prev))
                .getOutputs();
        prev = value;
        value = next;
      }
      return Output.create(value);
    }
  }
}
