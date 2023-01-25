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
          public void expand(SdkWorkflowBuilder builder) {
            builder.output("b", builder.inputOfBoolean("b"));
            builder.output("datetime", builder.inputOfDatetime("datetime"));
            builder.output("duration", builder.inputOfDuration("duration"));
            builder.output("f", builder.inputOfFloat("f"));
            builder.output("i", builder.inputOfInteger("i"));
            builder.output("s", builder.inputOfString("s"));
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
    SdkWorkflow<TestUnaryIntegerIO, Void> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryIntegerIO.class), SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.output("integer", builder.inputOfInteger("integer"));
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
          public void expand(SdkWorkflowBuilder builder) {
            builder.output("string", builder.inputOfString("string"));
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
          public void expand(SdkWorkflowBuilder builder) {
            builder.output("string", builder.inputOfString("string"));
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
          public void expand(SdkWorkflowBuilder builder) {
            builder.output("string", builder.inputOfString("string"));
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
  public void testWithTask_missingRemoteTask() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingData.ofInteger(1L), SdkBindingData.ofInteger(2L)));
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
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingData.ofInteger(1L), SdkBindingData.ofInteger(2L)));
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
                            SdkBindingData.ofInteger(10L), SdkBindingData.ofInteger(20L)),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=1}}}, type=LiteralType{simpleType=INTEGER}, value=1}, b=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=2}}}, type=LiteralType{simpleType=INTEGER}, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  @Test
  public void testWithTask_nullOutput() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply(
                "void", RemoteVoidOutputTask.create(), RemoteVoidOutputTask.Input.create(""));
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
                TestUnaryIntegerIO.create(SdkBindingData.ofInteger(7)),
                JacksonSdkType.of(TestUnaryIntegerIO.class),
                TestUnaryIntegerIO.create(SdkBindingData.ofInteger(5)))
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
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> a = builder.inputOfInteger("a");
            SdkBindingData<Long> b = builder.inputOfInteger("b");
            SdkBindingData<Long> c =
                builder
                    .apply("launchplanref", launchplanRef, SumLaunchPlanInput.create(a, b))
                    .getOutputs()
                    .c();

            builder.output("o", c);
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withFixedInput("a", 3L)
            .withFixedInput("b", 5L)
            .withLaunchPlanOutput(
                launchplanRef,
                SumLaunchPlanInput.create(
                    SdkBindingData.ofInteger(3L), SdkBindingData.ofInteger(5L)),
                SumLaunchPlanOutput.create(SdkBindingData.ofInteger(8L)))
            .execute();

    assertThat(result.getIntegerOutput("o"), equalTo(8L));
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
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> a = builder.inputOfInteger("a");
            SdkBindingData<Long> b = builder.inputOfInteger("b");
            SdkBindingData<Long> c =
                builder
                    .apply("launchplanref", launchplanRef, SumLaunchPlanInput.create(a, b))
                    .getOutputs()
                    .c();

            builder.output("integer", c);
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
                            SdkBindingData.ofInteger(100000L), SdkBindingData.ofInteger(100000L)),
                        SumLaunchPlanOutput.create(SdkBindingData.ofInteger(8L)))
                    .execute());

    assertThat(
        ex.getMessage(),
        equalTo(
            "Can't find input SumLaunchPlanInput{a=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=3}}}, type=LiteralType{simpleType=INTEGER}, value=3}, b=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=5}}}, type=LiteralType{simpleType=INTEGER}, value=5}} for remote launch plan [SumWorkflow] across known launch plan inputs, use SdkTestingExecutor#withLaunchPlanOutput or SdkTestingExecutor#withLaunchPlan to provide a test double"));
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
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> a = builder.inputOfInteger("a");
            SdkBindingData<Long> b = builder.inputOfInteger("b");
            SdkBindingData<Long> c =
                builder
                    .apply("launchplanref", launchplanRef, SumLaunchPlanInput.create(a, b))
                    .getOutputs()
                    .c();

            builder.output("integer", c);
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
                        SdkBindingData.ofInteger(in.a().get() + in.b().get())))
            .execute();

    assertThat(result.getIntegerOutput("integer"), equalTo(35L));
  }

  @Test
  public void testWithLaunchPlan_missingRemoteTaskOutput() {
    SdkWorkflow<Void, Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls(), SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply(
                "sum",
                RemoteSumTask.create(),
                RemoteSumInput.create(SdkBindingData.ofInteger(1L), SdkBindingData.ofInteger(2L)));
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
                            SdkBindingData.ofInteger(10L), SdkBindingData.ofInteger(20L)),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=1}}}, type=LiteralType{simpleType=INTEGER}, value=1}, b=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=2}}}, type=LiteralType{simpleType=INTEGER}, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  public static class SimpleUberWorkflow
      extends SdkWorkflow<TestUnaryIntegerIO, TestUnaryIntegerIO> {

    public SimpleUberWorkflow() {
      super(
          JacksonSdkType.of(TestUnaryIntegerIO.class), JacksonSdkType.of(TestUnaryIntegerIO.class));
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> input = builder.inputOfInteger("integer", "");
      SdkBindingData<Long> output =
          builder
              .apply("void", new SimpleSubWorkflow(), TestUnaryIntegerIO.create(input))
              .getOutputs()
              .integer();
      builder.output("integer", output);
    }
  }

  public static class SimpleSubWorkflow
      extends SdkWorkflow<TestUnaryIntegerIO, TestUnaryIntegerIO> {

    public SimpleSubWorkflow() {
      super(
          JacksonSdkType.of(TestUnaryIntegerIO.class), JacksonSdkType.of(TestUnaryIntegerIO.class));
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      builder.output("integer", builder.inputOfInteger("integer"));
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
