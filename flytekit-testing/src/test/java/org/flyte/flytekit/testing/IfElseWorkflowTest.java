/*
 * Copyright 2021-2023 Flyte Authors
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

import static org.flyte.flytekit.SdkConditions.eq;
import static org.flyte.flytekit.SdkConditions.gt;
import static org.flyte.flytekit.SdkConditions.lt;
import static org.flyte.flytekit.SdkConditions.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import java.util.stream.Stream;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkCondition;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.flyte.flytekit.testing.IfElseWorkflowTest.ConstStringTask.Output;
import org.flyte.flytekit.testing.SdkTestingExecutor.Result;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IfElseWorkflowTest {

  @ParameterizedTest
  @MethodSource("testCases")
  public void test(int a, int b, int c, int d, String expected) {
    // We don't support unit testing of branches nodes so we must be content just
    // catching the exception
    Result output =
        SdkTestingExecutor.of(new BranchNodeWorkflow())
            .withFixedInput("a", a)
            .withFixedInput("b", b)
            .withFixedInput("c", c)
            .withFixedInput("d", d)
            .withTask(new ConstStringTask(), in -> Output.create(in.value()))
            .execute();

    assertThat(output.getStringOutput("value"), equalTo(expected));
  }

  public static Stream<Arguments> testCases() {
    return Stream.of(
        Arguments.of(1, 2, 3, 4, "a < b && c < d"),
        Arguments.of(1, 2, 3, 3, "a < b && c == d"),
        Arguments.of(1, 2, 4, 3, "a < b && c > d"),
        Arguments.of(1, 1, 3, 4, "a == b && c < d"),
        Arguments.of(1, 1, 3, 3, "a == b && c == d"),
        Arguments.of(1, 1, 4, 3, "a == b && c > d"),
        Arguments.of(2, 1, 3, 4, "a > b && c < d"),
        Arguments.of(2, 1, 3, 3, "a > b && c == d"),
        Arguments.of(2, 1, 4, 3, "a > b && c > d"));
  }

  static class BranchNodeWorkflow
      extends SdkWorkflow<BranchNodeWorkflow.Input, ConstStringTask.Output> {

    @AutoValue
    public abstract static class Input {
      public abstract SdkBindingData<Long> a();

      public abstract SdkBindingData<Long> b();

      public abstract SdkBindingData<Long> c();

      public abstract SdkBindingData<Long> d();

      public static BranchNodeWorkflow.Input create(
          SdkBindingData<Long> a,
          SdkBindingData<Long> b,
          SdkBindingData<Long> c,
          SdkBindingData<Long> d) {
        return new AutoValue_IfElseWorkflowTest_BranchNodeWorkflow_Input(a, b, c, d);
      }
    }

    BranchNodeWorkflow() {
      super(
          JacksonSdkType.of(BranchNodeWorkflow.Input.class),
          JacksonSdkType.of(ConstStringTask.Output.class));
    }

    @Override
    public ConstStringTask.Output expand(
        SdkWorkflowBuilder builder, BranchNodeWorkflow.Input input) {
      SdkBindingData<Long> a = input.a();
      SdkBindingData<Long> b = input.b();
      SdkBindingData<Long> c = input.c();
      SdkBindingData<Long> d = input.d();

      SdkCondition<ConstStringTask.Output> condition =
          when(
                  "a == b",
                  eq(a, b),
                  when(
                          "c == d",
                          eq(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(
                              SdkBindingDataFactory.of("a == b && c == d")))
                      .when(
                          "c > d",
                          gt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a == b && c > d")))
                      .when(
                          "c < d",
                          lt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(
                              SdkBindingDataFactory.of("a == b && c < d"))))
              .when(
                  "a < b",
                  lt(a, b),
                  when(
                          "c == d",
                          eq(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c == d")))
                      .when(
                          "c > d",
                          gt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c > d")))
                      .when(
                          "c < d",
                          lt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c < d"))))
              .when(
                  "a > b",
                  gt(a, b),
                  when(
                          "c == d",
                          eq(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a > b && c == d")))
                      .when(
                          "c > d",
                          gt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(SdkBindingDataFactory.of("a > b && c > d")))
                      .when(
                          "c < d",
                          lt(c, d),
                          new ConstStringTask(),
                          ConstStringTask.Input.create(
                              SdkBindingDataFactory.of("a > b && c < d"))));

      SdkBindingData<String> value = builder.apply("condition", condition).getOutputs().value();

      return ConstStringTask.Output.create(value);
    }
  }

  static class ConstStringTask
      extends SdkRunnableTask<ConstStringTask.Input, ConstStringTask.Output> {
    private static final long serialVersionUID = 5553122612313564203L;

    @AutoValue
    abstract static class Input {
      abstract SdkBindingData<String> value();

      public static Input create(SdkBindingData<String> value) {
        return new AutoValue_IfElseWorkflowTest_ConstStringTask_Input(value);
      }
    }

    @AutoValue
    abstract static class Output {
      abstract SdkBindingData<String> value();

      public static Output create(SdkBindingData<String> value) {
        return new AutoValue_IfElseWorkflowTest_ConstStringTask_Output(value);
      }
    }

    public ConstStringTask() {
      super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
    }

    @Override
    public Output run(Input input) {
      return Output.create(input.value());
    }
  }
}
