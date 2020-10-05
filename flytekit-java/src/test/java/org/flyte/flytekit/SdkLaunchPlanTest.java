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
package org.flyte.flytekit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.time.Instant;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.junit.jupiter.api.Test;

class SdkLaunchPlanTest {

  @Test
  void shouldCreateLaunchPlansWithDefaultName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertThat(plan.getName(), equalTo("org.flyte.flytekit.SdkLaunchPlanTest$TestWorkflow"));
  }

  @Test
  void shouldCreateLaunchPlansWithOnlyWorkflowName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertAll(
        () -> assertThat(plan.getWorkflowProject(), nullValue()),
        () -> assertThat(plan.getWorkflowDomain(), nullValue()),
        () ->
            assertThat(
                plan.getWorkflowName(),
                equalTo("org.flyte.flytekit.SdkLaunchPlanTest$TestWorkflow")),
        () -> assertThat(plan.getWorkflowVersion(), nullValue()));
  }

  @Test
  void shouldCreateLaunchPlansWithNoInputsByDefault() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertThat(plan.getFixedInputs(), anEmptyMap());
  }

  @Test
  void shouldOverrideLaunchPlanName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow()).withName("new-workflow-name");

    assertThat(plan.getName(), equalTo("new-workflow-name"));
  }

  @Test
  void shouldCreateLaunchPlanWithCronSchedule() {
    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            .withCronSchedule(SdkCronSchedule.of("*/5 * * * *", Duration.ofHours(1)));

    assertThat(plan.getCronSchedule(), notNullValue());
    assertThat(plan.getCronSchedule().schedule(), equalTo("*/5 * * * *"));
    assertThat(plan.getCronSchedule().offset(), equalTo(Duration.ofHours(1)));
  }

  @Test
  void shouldAddFixedInputs() {
    @Var SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());
    Instant now = Instant.now();
    Duration duration = Duration.ofSeconds(123);

    plan =
        plan.withFixedInput("long", 123L)
            .withFixedInput("float", 1.23)
            .withFixedInput("string", "123")
            .withFixedInput("boolean", true)
            .withFixedInput("datetime", now)
            .withFixedInput("duration", duration)
            .withFixedInputs(SdkTypes.autoValue(Inputs.class), Inputs.create(456, 4.56));

    assertThat(
        plan.getFixedInputs(),
        allOf(
            hasEntry("long", asLiteral(Primitive.ofInteger(123))),
            hasEntry("float", asLiteral(Primitive.ofFloat(1.23))),
            hasEntry("string", asLiteral(Primitive.ofString("123"))),
            hasEntry("boolean", asLiteral(Primitive.ofBoolean(true))),
            hasEntry("datetime", asLiteral(Primitive.ofDatetime(now))),
            hasEntry("duration", asLiteral(Primitive.ofDuration(duration))),
            hasEntry("foo", asLiteral(Primitive.ofInteger(456))),
            hasEntry("bar", asLiteral(Primitive.ofFloat(4.56)))));
  }

  @Test
  void shouldRejectFixedInputDuplicates() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> plan.withFixedInput("input", "foo").withFixedInput("input", "bar"));

    assertThat(exception.getMessage(), containsString("Duplicate input [input]"));
  }

  private Literal asLiteral(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  private static class TestWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // nothing
    }
  }

  @AutoValue
  abstract static class Inputs {
    abstract long foo();

    abstract double bar();

    public static Inputs create(long foo, double bar) {
      return new AutoValue_SdkLaunchPlanTest_Inputs(foo, bar);
    }
  }
}
