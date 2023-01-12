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
  public abstract static class TestWorkflowOutput {

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> datetime();

    public abstract SdkBindingData<Duration> duration();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<String> s();
  }

  @AutoValue
  public abstract static class TestUnaryStringOutput {
    public abstract SdkBindingData<String> string();

    public static TestUnaryStringOutput create(String string) {
      return new AutoValue_SdkTestingExecutorTest_TestUnaryStringOutput(
          SdkBindingData.ofString(string));
    }
  }

  @AutoValue
  public abstract static class TestUnaryIntegerOutput {
    public abstract SdkBindingData<Long> o();

    public static TestUnaryIntegerOutput create(long l) {
      return new AutoValue_SdkTestingExecutorTest_TestUnaryIntegerOutput(
          SdkBindingData.ofInteger(l));
    }
  }

  @Test
  public void testPrimitiveTypes() {
    SdkWorkflow<TestWorkflowOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestWorkflowOutput.class)) {
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
    SdkWorkflow<Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls()) {
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
    SdkWorkflow<TestUnaryStringOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryStringOutput.class)) {
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
    SdkWorkflow<TestUnaryStringOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryStringOutput.class)) {
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
    SdkWorkflow<TestUnaryStringOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryStringOutput.class)) {
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
    SdkWorkflow<Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply("sum", RemoteSumTask.create().withInput("a", 1L).withInput("b", 2L));
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
    SdkWorkflow<Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply("sum", RemoteSumTask.create().withInput("a", 1L).withInput("b", 2L));
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SdkTestingExecutor.of(workflow)
                    .withTaskOutput(
                        RemoteSumTask.create(),
                        RemoteSumInput.create(10L, 20L),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=1}}}, type=LiteralType{simpleType=INTEGER}, value=1}, b=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=2}}}, type=LiteralType{simpleType=INTEGER}, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  @Test
  public void testWithTask_nullOutput() {
    SdkWorkflow<Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply("void", RemoteVoidOutputTask.create().withInput("ignore", ""));
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
            .withFixedInput("n", 7)
            .withWorkflowOutput(
                new SimpleSubWorkflow(),
                JacksonSdkType.of(SimpleSubWorkflowInput.class),
                SimpleSubWorkflowInput.create(7),
                JacksonSdkType.of(TestUnaryIntegerOutput.class),
                TestUnaryIntegerOutput.create(5))
            .execute();

    assertThat(result.getIntegerOutput("o"), equalTo(5L));
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

    SdkWorkflow<TestUnaryIntegerOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryIntegerOutput.class)) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef
                            .withInput("a", builder.inputOfInteger("a"))
                            .withInput("b", builder.inputOfInteger("b")))
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
                launchplanRef, SumLaunchPlanInput.create(3L, 5L), SumLaunchPlanOutput.create(8L))
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

    SdkWorkflow<TestUnaryIntegerOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryIntegerOutput.class)) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef
                            .withInput("a", builder.inputOfInteger("a"))
                            .withInput("b", builder.inputOfInteger("b")))
                    .getOutputs()
                    .c();

            builder.output("o", c);
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
                        SumLaunchPlanInput.create(100000L, 100000L),
                        SumLaunchPlanOutput.create(8L))
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

    SdkWorkflow<TestUnaryIntegerOutput> workflow =
        new SdkWorkflow<>(JacksonSdkType.of(TestUnaryIntegerOutput.class)) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            SdkBindingData<Long> c =
                builder
                    .apply(
                        "launchplanref",
                        launchplanRef
                            .withInput("a", builder.inputOfInteger("a"))
                            .withInput("b", builder.inputOfInteger("b")))
                    .getOutputs()
                    .c();

            builder.output("o", c);
          }
        };

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(workflow)
            .withFixedInput("a", 30L)
            .withFixedInput("b", 5L)
            .withLaunchPlan(
                launchplanRef, in -> SumLaunchPlanOutput.create(in.a().get() + in.b().get()))
            .execute();

    assertThat(result.getIntegerOutput("o"), equalTo(35L));
  }

  @Test
  public void testWithLaunchPlan_missingRemoteTaskOutput() {
    SdkWorkflow<Void> workflow =
        new SdkWorkflow<>(SdkTypes.nulls()) {
          @Override
          public void expand(SdkWorkflowBuilder builder) {
            builder.apply("sum", RemoteSumTask.create().withInput("a", 1L).withInput("b", 2L));
          }
        };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SdkTestingExecutor.of(workflow)
                    .withTaskOutput(
                        RemoteSumTask.create(),
                        RemoteSumInput.create(10L, 20L),
                        RemoteSumOutput.create(30L))
                    .execute());

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't find input RemoteSumInput{a=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=1}}}, type=LiteralType{simpleType=INTEGER}, value=1}, b=SdkBindingData{idl=BindingData{scalar=Scalar{primitive=Primitive{integerValue=2}}}, type=LiteralType{simpleType=INTEGER}, value=2}} for remote task [remote_sum_task] across known task inputs, use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask to provide a test double"));
  }

  public static class SimpleUberWorkflow extends SdkWorkflow<TestUnaryIntegerOutput> {

    public SimpleUberWorkflow() {
      super(JacksonSdkType.of(TestUnaryIntegerOutput.class));
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> input = builder.inputOfInteger("n", "");
      SdkBindingData<Long> output =
          builder.apply("void", new SimpleSubWorkflow().withInput("in", input)).getOutputs().o();
      builder.output("o", output);
    }
  }

  public static class SimpleSubWorkflow extends SdkWorkflow<TestUnaryIntegerOutput> {

    public SimpleSubWorkflow() {
      super(JacksonSdkType.of(TestUnaryIntegerOutput.class));
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      builder.output("o", builder.inputOfInteger("in"));
    }
  }

  @AutoValue
  abstract static class SimpleSubWorkflowInput {
    abstract SdkBindingData<Long> in();

    public static SimpleSubWorkflowInput create(long in) {
      return new AutoValue_SdkTestingExecutorTest_SimpleSubWorkflowInput(
          SdkBindingData.ofInteger(in));
    }
  }

  @AutoValue
  abstract static class SimpleSubWorkflowOutput {
    abstract SdkBindingData<Long> out();

    public static SimpleSubWorkflowOutput create(long out) {
      return new AutoValue_SdkTestingExecutorTest_SimpleSubWorkflowOutput(
          SdkBindingData.ofInteger(out));
    }
  }

  @AutoValue
  abstract static class SumLaunchPlanInput {
    abstract SdkBindingData<Long> a();

    abstract SdkBindingData<Long> b();

    public static SumLaunchPlanInput create(long a, long b) {
      return new AutoValue_SdkTestingExecutorTest_SumLaunchPlanInput(
          SdkBindingData.ofInteger(a), SdkBindingData.ofInteger(b));
    }
  }

  @AutoValue
  abstract static class SumLaunchPlanOutput {
    abstract SdkBindingData<Long> c();

    public static SumLaunchPlanOutput create(long c) {
      return new AutoValue_SdkTestingExecutorTest_SumLaunchPlanOutput(SdkBindingData.ofInteger(c));
    }
  }
}
