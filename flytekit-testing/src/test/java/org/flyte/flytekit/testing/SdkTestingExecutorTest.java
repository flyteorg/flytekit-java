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
package org.flyte.flytekit.testing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkRemoteLaunchPlan;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.flyte.flytekit.testing.RemoteSumTask.RemoteSumInput;
import org.flyte.flytekit.testing.RemoteSumTask.RemoteSumOutput;
import org.junit.jupiter.api.Test;

public class SdkTestingExecutorTest {

  @AutoValue
  public abstract static class TestWorkflowIO {

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> datetime();

    public abstract SdkBindingData<Duration> duration();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<String> s();

    public static TestWorkflowIO create(
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> datetime,
        SdkBindingData<Duration> duration,
        SdkBindingData<Double> f,
        SdkBindingData<Long> i,
        SdkBindingData<String> s) {
      return new AutoValue_SdkTestingExecutorTest_TestWorkflowIO(b, datetime, duration, f, i, s);
    }
  }

  @AutoValue
  public abstract static class TestUnaryStringIO {
    public abstract SdkBindingData<String> string();

    public static TestUnaryStringIO create(SdkBindingData<String> string) {
      return new AutoValue_SdkTestingExecutorTest_TestUnaryStringIO(string);
    }
  }

  @AutoValue
  public abstract static class TestUnaryIntegerIO {
    public abstract SdkBindingData<Long> integer();

    public static TestUnaryIntegerIO create(SdkBindingData<Long> integer) {
      return new AutoValue_SdkTestingExecutorTest_TestUnaryIntegerIO(integer);
    }
  }

  @Test
  public void testPrimitiveTypes() {
    SdkWorkflow<TestWorkflowIO, TestWorkflowIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(TestWorkflowIO.class), JacksonSdkType.of(TestWorkflowIO.class)) {
          @Override
          public TestWorkflowIO expand(SdkWorkflowBuilder builder, TestWorkflowIO input) {
            return TestWorkflowIO.create(
                input.b(), input.datetime(), input.duration(), input.f(), input.i(), input.s());
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withFixedInput("b", true)
            .withFixedInput("datetime", Instant.ofEpochSecond(1337))
            .withFixedInput("duration", Duration.ofDays(1))
            .withFixedInput("f", 1337.0)
            .withFixedInput("i", 42)
            .withFixedInput("s", "forty two")
            .execute();

    assertThat(result.getBooleanOutput("b"), equalTo(true));
    assertThat(result.getDatetimeOutput("datetime"), equalTo(Instant.ofEpochSecond(1337)));
    assertThat(result.getDurationOutput("duration"), equalTo(Duration.ofDays(1)));
    assertThat(result.getFloatOutput("f"), equalTo(1337.0));
    assertThat(result.getIntegerOutput("i"), equalTo(42L));
    assertThat(result.getStringOutput("s"), equalTo("forty two"));
  }

  @Test
  public void testGetOutput_doesntExist() {
    SdkWorkflow<TestUnaryIntegerIO, TestUnaryIntegerIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(TestUnaryIntegerIO.class),
            JacksonSdkType.of(TestUnaryIntegerIO.class)) {
          @Override
          public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, TestUnaryIntegerIO input) {
            return TestUnaryIntegerIO.create(input.integer());
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow).withFixedInput("integer", 42).execute();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> result.getStringOutput("string"));

    assertThat(e.getMessage(), equalTo("Output [string] doesn't exist in [integer]"));
  }

  @Test
  public void testGetOutput_illegalType() {
    SdkWorkflow<TestUnaryStringIO, TestUnaryStringIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(TestUnaryStringIO.class),
            JacksonSdkType.of(TestUnaryStringIO.class)) {
          @Override
          public TestUnaryStringIO expand(SdkWorkflowBuilder builder, TestUnaryStringIO input) {
            return TestUnaryStringIO.create(input.string());
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow).withFixedInput("string", "forty two").execute();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> result.getIntegerOutput("string"));

    assertThat(
        e.getMessage(),
        equalTo("Output [string] (type STRING) doesn't match expected type INTEGER"));
  }

  @Test
  public void testWithFixedInput_missing() {
    SdkWorkflow<TestUnaryStringIO, TestUnaryStringIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(TestUnaryStringIO.class),
            JacksonSdkType.of(TestUnaryStringIO.class)) {
          @Override
          public TestUnaryStringIO expand(SdkWorkflowBuilder builder, TestUnaryStringIO input) {
            return TestUnaryStringIO.create(input.string());
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SdkTestingExecutor.of(workflow).execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Fixed input [string] (of type STRING) isn't defined, use SdkTestingExecutor#withFixedInput"));
  }

  @Test
  public void testWithFixedInput_illegalType() {
    SdkWorkflow<TestUnaryStringIO, TestUnaryStringIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(TestUnaryStringIO.class),
            JacksonSdkType.of(TestUnaryStringIO.class)) {
          @Override
          public TestUnaryStringIO expand(SdkWorkflowBuilder builder, TestUnaryStringIO input) {
            return TestUnaryStringIO.create(input.string());
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> SdkTestingExecutor.of(workflow).withFixedInput("string", 42).execute());

    assertThat(
        e.getMessage(),
        equalTo("Fixed input [string] (of type INTEGER) doesn't match expected type STRING"));
  }

  @Test
  public void testWithTask_mockedRunnableTasksAreNotRun() {
    SdkWorkflow<Void, SumTask.SumOutput> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), JacksonSdkType.of(SumTask.SumOutput.class)) {
          @Override
          public SumTask.SumOutput expand(SdkWorkflowBuilder builder, Void noInput) {
            SdkBindingData<Long> sum =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();
            return SumTask.SumOutput.create(sum);
          }
        };

    long result =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)),
                // note the incorrect sum 1+1=3
                SumTask.SumOutput.create(SdkBindingDataFactory.of(3L)))
            .execute()
            .getIntegerOutput("c");

    assertThat(result, equalTo(3L));
  }

  @Test
  public void testWithTask_mocksCanBeUsedSeveralTimes() {
    // TODO: make this configurable, like mockito verify
    //  https://javadoc.io/doc/org.mockito/mockito-core/latest/org/mockito/Mockito.html#verify-T-
    SdkWorkflow<Void, SumTask.SumOutput> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), JacksonSdkType.of(SumTask.SumOutput.class)) {
          @Override
          public SumTask.SumOutput expand(SdkWorkflowBuilder builder, Void noInput) {
            SdkBindingData<Long> sum1 =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();
            SdkBindingData<Long> sum2 =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            // depend on earlier task to make sure mock has already been used once
                            sum1, SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();
            return SumTask.SumOutput.create(sum2);
          }
        };

    long result =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)),
                // 1+1=1 to make it hit the same mock
                SumTask.SumOutput.create(SdkBindingDataFactory.of(1L)))
            .execute()
            .getIntegerOutput("c");

    assertThat(result, equalTo(1L));
  }

  @Test
  public void testWithTask_missingMocksRunActualTaskIfNoMocksOfSameTaskAreDefined() {
    SdkWorkflow<Void, SumTask.SumOutput> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), JacksonSdkType.of(SumTask.SumOutput.class)) {
          @Override
          public SumTask.SumOutput expand(SdkWorkflowBuilder builder, Void noInput) {
            SdkBindingData<Long> unmockedSum =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();
            builder.apply(
                RemoteSumTask.create(),
                RemoteSumTask.RemoteSumInput.create(
                    SdkBindingDataFactory.of(2L), SdkBindingDataFactory.of(2L)));

            return SumTask.SumOutput.create(unmockedSum);
          }
        };

    SdkTestingExecutor executor =
        SdkTestingExecutor.of(workflow)
            // note: no mock for first task
            .withTaskOutput(
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingDataFactory.of(2L), SdkBindingDataFactory.of(2L)),
                RemoteSumOutput.create(4L));

    long result = executor.execute().getIntegerOutput("c");

    assertThat(result, equalTo(2L));
  }

  @Test
  public void testWithTask_missingMocksRunActualTaskIfNoMocksAtAllAreDefined() {
    SdkWorkflow<Void, SumTask.SumOutput> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), JacksonSdkType.of(SumTask.SumOutput.class)) {
          @Override
          public SumTask.SumOutput expand(SdkWorkflowBuilder builder, Void noInput) {
            SdkBindingData<Long> unmockedSum =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();

            return SumTask.SumOutput.create(unmockedSum);
          }
        };

    long result = SdkTestingExecutor.of(workflow).execute().getIntegerOutput("c");

    assertThat(result, equalTo(2L));
  }

  @Test
  public void testWithTask_missingMocksThrowWhenOtherMocksAreDefined() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            builder.apply(
                new SumTask(),
                SumTask.SumInput.create(
                    SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)));
            builder.apply(
                new SumTask(),
                SumTask.SumInput.create(
                    SdkBindingDataFactory.of(2L), SdkBindingDataFactory.of(2L)));

            return null;
          }
        };

    SdkTestingExecutor executor =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)),
                SumTask.SumOutput.create(SdkBindingDataFactory.of(2L)));

    assertThrows(IllegalArgumentException.class, executor::execute);
  }

  @Test
  public void testWithTask_duplicateMocksNotAllowed() {
    SdkWorkflow<Void, SumTask.SumOutput> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), JacksonSdkType.of(SumTask.SumOutput.class)) {
          @Override
          public SumTask.SumOutput expand(SdkWorkflowBuilder builder, Void noInput) {
            SdkBindingData<Long> sum =
                builder
                    .apply(
                        new SumTask(),
                        SumTask.SumInput.create(
                            SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)))
                    .getOutputs()
                    .c();
            return SumTask.SumOutput.create(sum);
          }
        };

    SdkTestingExecutor executor =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)),
                SumTask.SumOutput.create(SdkBindingDataFactory.of(3L)));

    assertThrows(
        TestingRunnableNode.DuplicateMockException.class,
        () ->
            executor
                // same input as above but other output
                .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(1L)),
                // note incorrect sum 1+1=4
                SumTask.SumOutput.create(SdkBindingDataFactory.of(4L))));
  }

  @Test
  public void testWithTask_unusedMocksThrow() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            return null;
          }
        };

    SdkTestingExecutor executor =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(2L)),
                SumTask.SumOutput.create(SdkBindingDataFactory.of(3L)));

    assertThrows(SdkTestingExecutor.UnusedMockException.class, executor::execute);
  }

  @Test
  public void testWithTask_missingRemoteTask() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(2L)));
            return null;
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SdkTestingExecutor.of(workflow).execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't execute remote task [remote_sum_task], use SdkTestingExecutor#withTaskOutput or "
                + "SdkTestingExecutor#withTask to provide a test double"));
  }

  @Test
  public void testWithTask_missingRemoteTaskOutput() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(2L)));
            return null;
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SdkTestingExecutor.of(workflow)
                    .withTaskOutput(
                        RemoteSumTask.create(),
                        RemoteSumInput.create(
                            SdkBindingDataFactory.of(10L), SdkBindingDataFactory.of(20L)),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{type=integers, value=1}, b=SdkBindingData{type=integers, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  @Test
  public void testWithTask_nullOutput() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            builder.apply(
                "void", RemoteVoidOutputTask.create(), RemoteVoidOutputTask.Input.create(""));
            return null;
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withTaskOutput(
                RemoteVoidOutputTask.create(), RemoteVoidOutputTask.Input.create(""), null)
            .execute();

    assertTrue(result.literalMap().isEmpty());
  }

  @Test
  public void withWorkflowOutput_successfullyMocksWhenTypeMatches() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new SimpleUberWorkflow())
            .withFixedInput("integer", 7)
            .withWorkflowOutput(
                new SimpleSubWorkflow(),
                JacksonSdkType.of(TestUnaryIntegerIO.class),
                TestUnaryIntegerIO.create(SdkBindingDataFactory.of(7)),
                JacksonSdkType.of(TestUnaryIntegerIO.class),
                TestUnaryIntegerIO.create(SdkBindingDataFactory.of(5)))
            .execute();

    assertThat(result.getIntegerOutput("integer"), equalTo(5L));
  }

  @Test
  public void testWithLaunchPlanOutput() {
    SdkRemoteLaunchPlan<SumLaunchPlanInput, SumLaunchPlanOutput> launchplanRef =
        SdkRemoteLaunchPlan.create(
            "development",
            "flyte-warehouse",
            "SumWorkflow",
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(SumLaunchPlanOutput.class));

    SdkWorkflow<SumLaunchPlanInput, TestUnaryIntegerIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(TestUnaryIntegerIO.class)) {
          @Override
          public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, SumLaunchPlanInput input) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef,
                        SumLaunchPlanInput.create(input.a(), input.b()))
                    .getOutputs()
                    .c();

            return TestUnaryIntegerIO.create(c);
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withFixedInput("a", 3L)
            .withFixedInput("b", 5L)
            .withLaunchPlanOutput(
                launchplanRef,
                SumLaunchPlanInput.create(
                    SdkBindingDataFactory.of(3L), SdkBindingDataFactory.of(5L)),
                SumLaunchPlanOutput.create(SdkBindingDataFactory.of(8L)))
            .execute();

    assertThat(result.getIntegerOutput("integer"), equalTo(8L));
  }

  @Test
  public void testWithLaunchPlanOutput_isMissing() {
    SdkRemoteLaunchPlan<SumLaunchPlanInput, SumLaunchPlanOutput> launchplanRef =
        SdkRemoteLaunchPlan.create(
            "development",
            "flyte-warehouse",
            "SumWorkflow",
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(SumLaunchPlanOutput.class));

    SdkWorkflow<SumLaunchPlanInput, TestUnaryIntegerIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(TestUnaryIntegerIO.class)) {
          @Override
          public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, SumLaunchPlanInput input) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef,
                        SumLaunchPlanInput.create(input.a(), input.b()))
                    .getOutputs()
                    .c();

            return TestUnaryIntegerIO.create(c);
          }
        };

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SdkTestingExecutor.of(workflow)
                    .withFixedInput("a", 3L)
                    .withFixedInput("b", 5L)
                    .withLaunchPlanOutput(
                        launchplanRef,
                        // The stub values won't be matched, so exception iis throws
                        SumLaunchPlanInput.create(
                            SdkBindingDataFactory.of(100000L), SdkBindingDataFactory.of(100000L)),
                        SumLaunchPlanOutput.create(SdkBindingDataFactory.of(8L)))
                    .execute());

    assertThat(
        ex.getMessage(),
        equalTo(
            "Can't find input SumLaunchPlanInput{a=SdkBindingData{type=integers, value=3}, b=SdkBindingData{type=integers, value=5}} for remote launch plan [SumWorkflow] across known launch plan inputs, use SdkTestingExecutor#withLaunchPlanOutput or SdkTestingExecutor#withLaunchPlan to provide a test double"));
  }

  @Test
  public void testWithLaunchPlan() {
    SdkRemoteLaunchPlan<SumLaunchPlanInput, SumLaunchPlanOutput> launchplanRef =
        SdkRemoteLaunchPlan.create(
            "development",
            "flyte-warehouse",
            "SumWorkflow",
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(SumLaunchPlanOutput.class));

    SdkWorkflow<SumLaunchPlanInput, TestUnaryIntegerIO> workflow =
        new SdkWorkflow<>(
            JacksonSdkType.of(SumLaunchPlanInput.class),
            JacksonSdkType.of(TestUnaryIntegerIO.class)) {
          @Override
          public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, SumLaunchPlanInput input) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef,
                        SumLaunchPlanInput.create(input.a(), input.b()))
                    .getOutputs()
                    .c();

            return TestUnaryIntegerIO.create(c);
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withFixedInput("a", 30L)
            .withFixedInput("b", 5L)
            .withLaunchPlan(
                launchplanRef,
                in ->
                    SumLaunchPlanOutput.create(
                        SdkBindingDataFactory.of(in.a().get() + in.b().get())))
            .execute();

    assertThat(result.getIntegerOutput("integer"), equalTo(35L));
  }

  @Test
  public void testWithLaunchPlan_missingRemoteTaskOutput() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public Void expand(SdkWorkflowBuilder builder, Void noInput) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingDataFactory.of(1L), SdkBindingDataFactory.of(2L)));
            return null;
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SdkTestingExecutor.of(workflow)
                    .withTaskOutput(
                        RemoteSumTask.create(),
                        RemoteSumInput.create(
                            SdkBindingDataFactory.of(10L), SdkBindingDataFactory.of(20L)),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{type=integers, value=1}, b=SdkBindingData{type=integers, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  public static class SimpleUberWorkflow
      extends SdkWorkflow<TestUnaryIntegerIO, TestUnaryIntegerIO> {

    public SimpleUberWorkflow() {
      super(
          JacksonSdkType.of(TestUnaryIntegerIO.class), JacksonSdkType.of(TestUnaryIntegerIO.class));
    }

    @Override
    public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, TestUnaryIntegerIO input) {
      SdkBindingData<Long> output =
          builder
              .apply("void", new SimpleSubWorkflow(), TestUnaryIntegerIO.create(input.integer()))
              .getOutputs()
              .integer();
      return TestUnaryIntegerIO.create(output);
    }
  }

  public static class SimpleSubWorkflow
      extends SdkWorkflow<TestUnaryIntegerIO, TestUnaryIntegerIO> {

    public SimpleSubWorkflow() {
      super(
          JacksonSdkType.of(TestUnaryIntegerIO.class), JacksonSdkType.of(TestUnaryIntegerIO.class));
    }

    @Override
    public TestUnaryIntegerIO expand(SdkWorkflowBuilder builder, TestUnaryIntegerIO input) {
      return TestUnaryIntegerIO.create(input.integer());
    }
  }

  @AutoValue
  abstract static class SimpleSubWorkflowOutput {
    abstract SdkBindingData<Long> out();

    public static SimpleSubWorkflowOutput create(SdkBindingData<Long> out) {
      return new AutoValue_SdkTestingExecutorTest_SimpleSubWorkflowOutput(out);
    }
  }

  @AutoValue
  abstract static class SumLaunchPlanInput {
    abstract SdkBindingData<Long> a();

    abstract SdkBindingData<Long> b();

    public static SumLaunchPlanInput create(SdkBindingData<Long> a, SdkBindingData<Long> b) {
      return new AutoValue_SdkTestingExecutorTest_SumLaunchPlanInput(a, b);
    }
  }

  @AutoValue
  abstract static class SumLaunchPlanOutput {
    abstract SdkBindingData<Long> c();

    public static SumLaunchPlanOutput create(SdkBindingData<Long> c) {
      return new AutoValue_SdkTestingExecutorTest_SumLaunchPlanOutput(c);
    }
  }
}
