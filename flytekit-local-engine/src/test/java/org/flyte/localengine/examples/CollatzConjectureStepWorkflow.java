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
package org.flyte.localengine.examples;

import static org.flyte.flytekit.SdkBindingData.ofInteger;
import static org.flyte.flytekit.SdkConditions.isTrue;
import static org.flyte.flytekit.SdkConditions.when;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.NopNamedOutput;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

// if x is even, then x/2 else 3x+1
@AutoService(SdkWorkflow.class)
public class CollatzConjectureStepWorkflow extends SdkWorkflow<NopNamedOutput> {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData x = builder.inputOfInteger("x");
    SdkBindingData isOdd =
        builder.apply("is_odd", new IsEvenTask().withInput("x", x)).getOutput("res");

    SdkBindingData nextX =
        builder
            .apply(
                "decide",
                when(
                        "was_even",
                        isTrue(isOdd),
                        new Divide().withInput("num", x).withInput("den", ofInteger(2L)))
                    .otherwise("was_odd", new ThreeXPlusOne().withInput("x", x)))
            .getOutput("res");

    builder.output("nextX", nextX);
  }

  @AutoService(SdkRunnableTask.class)
  public static class IsEvenTask
      extends SdkRunnableTask<IsEvenTask.Input, IsEvenTask.Output, NopNamedOutput> {
    private static final long serialVersionUID = -1606085903949620311L;

    public IsEvenTask() {
      super(JacksonSdkType.of(IsEvenTask.Input.class), JacksonSdkType.of(IsEvenTask.Output.class));
    }

    @Override
    public IsEvenTask.Output run(IsEvenTask.Input input) {
      return IsEvenTask.Output.create(input.x() % 2 == 0);
    }

    @AutoValue
    public abstract static class Input {

      public abstract Long x();

      public static Input create(Long x) {
        return new AutoValue_CollatzConjectureStepWorkflow_IsEvenTask_Input(x);
      }
    }

    @AutoValue
    public abstract static class Output {

      public abstract boolean res();

      public static Output create(boolean res) {
        return new AutoValue_CollatzConjectureStepWorkflow_IsEvenTask_Output(res);
      }
    }
  }

  @AutoService(SdkRunnableTask.class)
  public static class Divide extends SdkRunnableTask<Divide.Input, Divide.Output, NopNamedOutput> {
    private static final long serialVersionUID = -526903889896397227L;

    public Divide() {
      super(JacksonSdkType.of(Divide.Input.class), JacksonSdkType.of(Divide.Output.class));
    }

    @Override
    public Divide.Output run(Divide.Input input) {
      return Divide.Output.create(input.num() / input.den());
    }

    @AutoValue
    public abstract static class Input {
      public abstract long num();

      public abstract long den();

      public static Input create(long num, long den) {
        return new AutoValue_CollatzConjectureStepWorkflow_Divide_Input(num, den);
      }
    }

    @AutoValue
    public abstract static class Output {

      public abstract long res();

      public static Output create(long res) {
        return new AutoValue_CollatzConjectureStepWorkflow_Divide_Output(res);
      }
    }
  }

  // 3x+1
  @AutoService(SdkRunnableTask.class)
  public static class ThreeXPlusOne
      extends SdkRunnableTask<ThreeXPlusOne.Input, ThreeXPlusOne.Output, NopNamedOutput> {
    private static final long serialVersionUID = 932934331328064751L;

    public ThreeXPlusOne() {
      super(
          JacksonSdkType.of(ThreeXPlusOne.Input.class),
          JacksonSdkType.of(ThreeXPlusOne.Output.class));
    }

    @Override
    public ThreeXPlusOne.Output run(ThreeXPlusOne.Input input) {
      return ThreeXPlusOne.Output.create(3 * input.x() + 1);
    }

    @AutoValue
    public abstract static class Input {
      public abstract long x();

      public static Input create(long x) {
        return new AutoValue_CollatzConjectureStepWorkflow_ThreeXPlusOne_Input(x);
      }
    }

    @AutoValue
    public abstract static class Output {

      public abstract long res();

      public static Output create(long res) {
        return new AutoValue_CollatzConjectureStepWorkflow_ThreeXPlusOne_Output(res);
      }
    }
  }
}
