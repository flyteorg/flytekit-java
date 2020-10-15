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
package org.flyte.flytekit.testing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.flyte.flytekit.testing.RemoteSumTask.RemoteSumInput;
import org.flyte.flytekit.testing.RemoteSumTask.RemoteSumOutput;
import org.flyte.flytekit.testing.SumTask.SumInput;
import org.flyte.flytekit.testing.SumTask.SumOutput;
import org.junit.jupiter.api.Test;

public class FibonacciWorkflowTest {

  @Test
  public void test() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new FibonacciWorkflow())
            .withFixedInput("fib0", 1)
            .withFixedInput("fib1", 1)
            .execute();

    assertThat(result.getIntegerOutput("fib2"), equalTo(2L));
    assertThat(result.getIntegerOutput("fib3"), equalTo(3L));
    assertThat(result.getIntegerOutput("fib4"), equalTo(5L));
    assertThat(result.getIntegerOutput("fib5"), equalTo(8L));
  }

  @Test
  public void testWithFixedInputs() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new FibonacciWorkflow())
            .withFixedInputs(
                JacksonSdkType.of(FibonacciWorkflowInputs.class),
                FibonacciWorkflowInputs.create(1, 1))
            .execute();

    assertThat(result.getIntegerOutput("fib2"), equalTo(2L));
    assertThat(result.getIntegerOutput("fib3"), equalTo(3L));
    assertThat(result.getIntegerOutput("fib4"), equalTo(5L));
    assertThat(result.getIntegerOutput("fib5"), equalTo(8L));
  }

  @Test
  public void testWithTaskOutput_runnableTask() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new FibonacciWorkflow())
            .withFixedInput("fib0", 1)
            .withFixedInput("fib1", 1)
            .withTaskOutput(new SumTask(), SumInput.create(3L, 5L), SumOutput.create(42L))
            .execute();

    assertThat(result.getIntegerOutput("fib2"), equalTo(2L));
    assertThat(result.getIntegerOutput("fib3"), equalTo(3L));
    assertThat(result.getIntegerOutput("fib4"), equalTo(5L));
    assertThat(result.getIntegerOutput("fib5"), equalTo(42L));
  }

  @Test
  public void testWithTaskOutput_remoteTask() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new RemoteFibonacciWorkflow())
            .withFixedInput("fib0", 1)
            .withFixedInput("fib1", 1)
            .withTaskOutput(
                RemoteSumTask.create(), RemoteSumInput.create(1L, 1L), RemoteSumOutput.create(5L))
            .withTaskOutput(
                RemoteSumTask.create(), RemoteSumInput.create(1L, 5L), RemoteSumOutput.create(10L))
            .withTaskOutput(
                RemoteSumTask.create(), RemoteSumInput.create(5L, 10L), RemoteSumOutput.create(20L))
            .withTaskOutput(
                RemoteSumTask.create(),
                RemoteSumInput.create(10L, 20L),
                RemoteSumOutput.create(40L))
            .execute();

    assertThat(result.getIntegerOutput("fib2"), equalTo(5L));
    assertThat(result.getIntegerOutput("fib3"), equalTo(10L));
    assertThat(result.getIntegerOutput("fib4"), equalTo(20L));
    assertThat(result.getIntegerOutput("fib5"), equalTo(40L));
  }

  @Test
  public void testWithTask() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new FibonacciWorkflow())
            .withFixedInput("fib0", 1)
            .withFixedInput("fib1", 1)
            .withTask(new SumTask(), input -> SumOutput.create(input.a() * input.b()))
            // can combine withTask and withTaskOutput
            .withTaskOutput(new SumTask(), SumInput.create(1, 1), SumOutput.create(2))
            .execute();

    assertThat(result.getIntegerOutput("fib2"), equalTo(2L));
    assertThat(result.getIntegerOutput("fib3"), equalTo(2L));
    assertThat(result.getIntegerOutput("fib4"), equalTo(4L));
    assertThat(result.getIntegerOutput("fib5"), equalTo(8L));
  }

  public static class FibonacciWorkflow extends SdkWorkflow {
    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData fib0 = builder.inputOfInteger("fib0");
      SdkBindingData fib1 = builder.inputOfInteger("fib1");

      SdkNode fib2 =
          builder.apply("fib-2", new SumTask().withInput("a", fib0).withInput("b", fib1));

      SdkNode fib3 =
          builder.apply(
              "fib-3", new SumTask().withInput("a", fib1).withInput("b", fib2.getOutput("c")));

      SdkNode fib4 =
          builder.apply(
              "fib-4",
              new SumTask()
                  .withInput("a", fib2.getOutput("c"))
                  .withInput("b", fib3.getOutput("c")));

      SdkNode fib5 =
          builder.apply(
              "fib-5",
              new SumTask()
                  .withInput("a", fib3.getOutput("c"))
                  .withInput("b", fib4.getOutput("c")));

      builder.output("fib2", fib2.getOutput("c"));
      builder.output("fib3", fib3.getOutput("c"));
      builder.output("fib4", fib4.getOutput("c"));
      builder.output("fib5", fib5.getOutput("c"));
    }
  }

  @AutoValue
  @JsonSerialize
  public abstract static class FibonacciWorkflowInputs {
    public abstract long getFib0();

    public abstract long getFib1();

    @JsonCreator
    public static FibonacciWorkflowInputs create(long fib0, long fib1) {
      return new AutoValue_FibonacciWorkflowTest_FibonacciWorkflowInputs(fib0, fib1);
    }
  }

  /** FibonacciWorkflow, but using RemoteSumTask instead. */
  public static class RemoteFibonacciWorkflow extends SdkWorkflow {
    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData fib0 = builder.inputOfInteger("fib0");
      SdkBindingData fib1 = builder.inputOfInteger("fib1");

      SdkNode fib2 =
          builder.apply("fib-2", RemoteSumTask.create().withInput("a", fib0).withInput("b", fib1));

      SdkNode fib3 =
          builder.apply(
              "fib-3",
              RemoteSumTask.create().withInput("a", fib1).withInput("b", fib2.getOutput("c")));

      SdkNode fib4 =
          builder.apply(
              "fib-4",
              RemoteSumTask.create()
                  .withInput("a", fib2.getOutput("c"))
                  .withInput("b", fib3.getOutput("c")));

      SdkNode fib5 =
          builder.apply(
              "fib-5",
              RemoteSumTask.create()
                  .withInput("a", fib3.getOutput("c"))
                  .withInput("b", fib4.getOutput("c")));

      builder.output("fib2", fib2.getOutput("c"));
      builder.output("fib3", fib3.getOutput("c"));
      builder.output("fib4", fib4.getOutput("c"));
      builder.output("fib5", fib5.getOutput("c"));
    }
  }
}
