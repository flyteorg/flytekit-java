/*
 * Copyright 2025 Flyte Authors.
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
package org.flyte.flytekit.testing;

import static org.flyte.flytekit.SdkBindingDataFactory.of;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.*;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SumIfEvenDynamicWorkflowTask.class)
public class SumIfEvenDynamicWorkflowTask
    extends SdkDynamicWorkflowTask<
        SumIfEvenDynamicWorkflowTask.Input, SumIfEvenDynamicWorkflowTask.Output> {
  @AutoValue
  public abstract static class Input {

    abstract SdkBindingData<Long> a();

    abstract SdkBindingData<Long> b();

    static Input create(SdkBindingData<Long> a, SdkBindingData<Long> b) {
      return new AutoValue_SumIfEvenDynamicWorkflowTask_Input(a, b);
    }
  }

  @AutoValue
  public abstract static class Output {

    abstract SdkBindingData<Long> c();

    static Output create(SdkBindingData<Long> c) {
      return new AutoValue_SumIfEvenDynamicWorkflowTask_Output(c);
    }
  }

  public SumIfEvenDynamicWorkflowTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(SdkWorkflowBuilder builder, Input input) {
    /*
     * This is to demonstrate that we can use concrete values in the dynamic workflow task
     */
    long aConcreteValue = input.a().get();
    long bConcreteValue = input.b().get();

    SumTask.SumOutput outputs =
        builder
            .apply(
                SdkConditions.when(
                        "is-even",
                        SdkConditions.isTrue(
                            of(aConcreteValue % 2 == 0 && bConcreteValue % 2 == 0)),
                        new SumTask(),
                        SumTask.SumInput.create(input.a(), input.b()))
                    .otherwise("is-odd", new SumTask(), SumTask.SumInput.create(of(0L), of(0L))))
            .getOutputs();

    return Output.create(outputs.c());
  }
}
