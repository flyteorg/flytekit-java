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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Parameter;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SdkLaunchPlanTest {

  @Test
  void shouldCreateLaunchPlansWithDefaultName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertThat(plan.name(), equalTo("org.flyte.flytekit.SdkLaunchPlanTest$TestWorkflow"));
  }

  @Test
  void shouldCreateLaunchPlansWithOnlyWorkflowName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertAll(
        () -> assertThat(plan.workflowProject(), nullValue()),
        () -> assertThat(plan.workflowDomain(), nullValue()),
        () ->
            assertThat(
                plan.workflowName(), equalTo("org.flyte.flytekit.SdkLaunchPlanTest$TestWorkflow")),
        () -> assertThat(plan.workflowVersion(), nullValue()));
  }

  @Test
  void shouldCreateLaunchPlansWithNoInputsByDefault() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    assertThat(plan.fixedInputs(), anEmptyMap());
  }

  @Test
  void shouldOverrideLaunchPlanName() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow()).withName("new-workflow-name");

    assertThat(plan.name(), equalTo("new-workflow-name"));
  }

  @Test
  void shouldCreateLaunchPlanWithCronSchedule() {
    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            .withCronSchedule(SdkCronSchedule.of("*/5 * * * *", Duration.ofHours(1)));

    assertThat(plan.cronSchedule(), notNullValue());
    assertThat(plan.cronSchedule().schedule(), equalTo("*/5 * * * *"));
    assertThat(plan.cronSchedule().offset(), equalTo(Duration.ofHours(1)));
  }

  @Test
  void shouldAddFixedInputs() {
    Instant now = Instant.now();
    Duration duration = Duration.ofSeconds(123);

    Map<String, Literal> fixedInputs = new HashMap<>();
    fixedInputs.put("inputsFoo", Literals.ofInteger(456));
    fixedInputs.put("inputsBar", Literals.ofFloat(4.56));

    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            .withFixedInput("integer", 123L)
            .withFixedInput("float", 1.23)
            .withFixedInput("string", "123")
            .withFixedInput("boolean", true)
            .withFixedInput("datetime", now)
            .withFixedInput("duration", duration)
            .withFixedInputs(
                TestSdkType.of("inputsFoo", LiteralTypes.INTEGER, "inputsBar", LiteralTypes.FLOAT),
                fixedInputs);

    assertThat(
        plan.fixedInputs(),
        allOf(
            hasEntry("integer", asLiteral(Primitive.ofIntegerValue(123))),
            hasEntry("float", asLiteral(Primitive.ofFloatValue(1.23))),
            hasEntry("string", asLiteral(Primitive.ofStringValue("123"))),
            hasEntry("boolean", asLiteral(Primitive.ofBooleanValue(true))),
            hasEntry("datetime", asLiteral(Primitive.ofDatetime(now))),
            hasEntry("duration", asLiteral(Primitive.ofDuration(duration))),
            hasEntry("inputsFoo", asLiteral(Primitive.ofIntegerValue(456))),
            hasEntry("inputsBar", asLiteral(Primitive.ofFloatValue(4.56)))));
  }

  @Test
  void shouldAddDefaultInputs() {
    Instant now = Instant.now();
    Duration duration = Duration.ofSeconds(123);

    Map<String, Literal> defaultInputs = new HashMap<>();
    defaultInputs.put("inputsFoo", Literals.ofInteger(456));
    defaultInputs.put("inputsBar", Literals.ofFloat(4.56));

    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            .withDefaultInput("integer", 123L)
            .withDefaultInput("float", 1.23)
            .withDefaultInput("string", "123")
            .withDefaultInput("boolean", true)
            .withDefaultInput("datetime", now)
            .withDefaultInput("duration", duration)
            .withDefaultInput(
                TestSdkType.of("inputsFoo", LiteralTypes.INTEGER, "inputsBar", LiteralTypes.FLOAT),
                defaultInputs);

    assertThat(
        plan.defaultInputs(),
        allOf(
            hasEntry("integer", asParameter(Primitive.ofIntegerValue(123), SimpleType.INTEGER)),
            hasEntry("float", asParameter(Primitive.ofFloatValue(1.23), SimpleType.FLOAT)),
            hasEntry("string", asParameter(Primitive.ofStringValue("123"), SimpleType.STRING)),
            hasEntry("boolean", asParameter(Primitive.ofBooleanValue(true), SimpleType.BOOLEAN)),
            hasEntry("datetime", asParameter(Primitive.ofDatetime(now), SimpleType.DATETIME)),
            hasEntry(
                "duration", asParameter(Primitive.ofDuration(duration), SimpleType.DURATION))));
  }

  @Test
  void shouldRejectDefaultInputType() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> plan.withDefaultInput("integer", 1D));

    assertThat(
        exception.getMessage(),
        containsString(
            "invalid default input wrong type integer, expected LiteralType{simpleType=INTEGER}, got LiteralType{simpleType=FLOAT}"));
  }

  @Test
  void shouldRejectFixedInputDuplicates() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> plan.withFixedInput("string", "foo").withFixedInput("string", "bar"));

    assertThat(exception.getMessage(), containsString("Duplicate fixed input [string]"));
  }

  @Test
  void shouldTypeCheckFixedInputNamesAgainstWorkflowInterfaceNames() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> plan.withFixedInput("notWorkflowInput", 0L));

    assertThat(exception.getMessage(), equalTo("unexpected fixed input notWorkflowInput"));
  }

  @Test
  void shouldTypeCheckFixedInputNamesAgainstEmptyWorkflowInterface() {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new NoInputsTestWorkflow());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> plan.withFixedInput("notWorkflowInput", 0L));

    assertThat(
        exception.getMessage(),
        equalTo("invalid launch plan fixed inputs, expected none but found 1"));
  }

  @ParameterizedTest
  @MethodSource("paramsForShouldTypeCheckFixedInputAgainstWorkflowInterface")
  void shouldTypeCheckFixedInputTypesAgainstWorkflowInterfaceTypes(Consumer<SdkLaunchPlan> fn) {
    SdkLaunchPlan plan = SdkLaunchPlan.of(new TestWorkflow());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> fn.accept(plan));

    assertThat(exception.getMessage(), containsString("invalid fixed input wrong type"));
  }

  static Stream<Consumer<SdkLaunchPlan>>
      paramsForShouldTypeCheckFixedInputAgainstWorkflowInterface() {
    return Stream.of(
        plan -> plan.withFixedInput("float", 0L),
        plan -> plan.withFixedInput("string", 0.0),
        plan -> plan.withFixedInput("boolean", "not a boolean"),
        plan -> plan.withFixedInput("datetime", false),
        plan -> plan.withFixedInput("duration", Instant.now()),
        plan -> plan.withFixedInput("integer", Duration.ZERO),
        plan -> plan.withFixedInput("inputsFoo", "not a integer"),
        plan -> plan.withFixedInput("inputsBar", "not a float"));
  }

  private Literal asLiteral(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  private Parameter asParameter(Primitive primitive, SimpleType simpleType) {
    return Parameter.create(
        Variable.builder()
            .description("")
            .literalType(LiteralType.ofSimpleType(simpleType))
            .build(),
        Literal.ofScalar(Scalar.ofPrimitive(primitive)));
  }

  private static class TestWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      builder.inputOfInteger("integer");
      builder.inputOfFloat("float");
      builder.inputOfString("string");
      builder.inputOfBoolean("boolean");
      builder.inputOfDatetime("datetime");
      builder.inputOfDuration("duration");
      builder.inputOfInteger("inputsFoo");
      builder.inputOfFloat("inputsBar");
    }
  }

  private static class NoInputsTestWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // no inputs
    }
  }
}
