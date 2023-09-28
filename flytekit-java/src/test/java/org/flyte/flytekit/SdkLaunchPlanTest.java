/*
 * Copyright 2020-2023 Flyte Authors
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
import java.time.Duration;
import java.time.Instant;
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

    TestPairIntegerInput fixedInputs =
        TestPairIntegerInput.create(SdkBindingDataFactory.of(456), SdkBindingDataFactory.of(789));

    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            .withFixedInput("integer", 123L)
            .withFixedInput("_float", 1.23)
            .withFixedInput("string", "123")
            .withFixedInput("_boolean", true)
            .withFixedInput("datetime", now)
            .withFixedInput("duration", duration)
            .withFixedInputs(new TestPairIntegerInput.SdkType(), fixedInputs);

    assertThat(
        plan.fixedInputs(),
        allOf(
            hasEntry("integer", asLiteral(Primitive.ofIntegerValue(123))),
            hasEntry("_float", asLiteral(Primitive.ofFloatValue(1.23))),
            hasEntry("string", asLiteral(Primitive.ofStringValue("123"))),
            hasEntry("_boolean", asLiteral(Primitive.ofBooleanValue(true))),
            hasEntry("datetime", asLiteral(Primitive.ofDatetime(now))),
            hasEntry("duration", asLiteral(Primitive.ofDuration(duration))),
            hasEntry("a", asLiteral(Primitive.ofIntegerValue(456))),
            hasEntry("b", asLiteral(Primitive.ofIntegerValue(789)))));
  }

  @Test
  void shouldAddDefaultInputs() {
    Instant now = Instant.now();
    Duration duration = Duration.ofSeconds(123);

    TestPairIntegerInput fixedInputs =
        TestPairIntegerInput.create(SdkBindingDataFactory.of(456), SdkBindingDataFactory.of(789));

    SdkLaunchPlan plan =
        SdkLaunchPlan.of(new TestWorkflow())
            // 😔 this is still untyped but the whole point is to be able to partially specify
            // inputs
            .withDefaultInput("integer", 123L)
            .withDefaultInput("_float", 1.23)
            .withDefaultInput("string", "123")
            .withDefaultInput("_boolean", true)
            .withDefaultInput("datetime", now)
            .withDefaultInput("duration", duration)
            .withDefaultInput(new TestPairIntegerInput.SdkType(), fixedInputs);

    assertThat(
        plan.defaultInputs(),
        allOf(
            hasEntry("integer", asParameter(Primitive.ofIntegerValue(123), SimpleType.INTEGER)),
            hasEntry("_float", asParameter(Primitive.ofFloatValue(1.23), SimpleType.FLOAT)),
            hasEntry("string", asParameter(Primitive.ofStringValue("123"), SimpleType.STRING)),
            hasEntry("_boolean", asParameter(Primitive.ofBooleanValue(true), SimpleType.BOOLEAN)),
            hasEntry("datetime", asParameter(Primitive.ofDatetime(now), SimpleType.DATETIME)),
            hasEntry("duration", asParameter(Primitive.ofDuration(duration), SimpleType.DURATION)),
            hasEntry("a", asParameter(Primitive.ofIntegerValue(456), SimpleType.INTEGER)),
            hasEntry("b", asParameter(Primitive.ofIntegerValue(789), SimpleType.INTEGER))));
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
        plan -> plan.withFixedInput("_float", 0L),
        plan -> plan.withFixedInput("string", 0.0),
        plan -> plan.withFixedInput("_boolean", "not a boolean"),
        plan -> plan.withFixedInput("datetime", false),
        plan -> plan.withFixedInput("duration", Instant.now()),
        plan -> plan.withFixedInput("integer", Duration.ZERO),
        plan -> plan.withFixedInput("a", "not a integer"),
        plan -> plan.withFixedInput("b", "not a integer"));
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

  private static class TestWorkflow extends SdkWorkflow<TestWorkflowInput, Void> {

    private TestWorkflow() {
      super(new TestWorkflowInput.SdkType(), SdkTypes.nulls());
    }

    @Override
    public Void expand(SdkWorkflowBuilder builder, TestWorkflowInput input) {
      return null;
    }
  }

  @AutoValue
  abstract static class TestWorkflowInput {
    abstract SdkBindingData<Long> integer();

    abstract SdkBindingData<Double> _float();

    abstract SdkBindingData<String> string();

    abstract SdkBindingData<Boolean> _boolean();

    abstract SdkBindingData<Instant> datetime();

    abstract SdkBindingData<Duration> duration();

    abstract SdkBindingData<Long> a();

    abstract SdkBindingData<Long> b();

    public static TestWorkflowInput create(
        SdkBindingData<Long> integer,
        SdkBindingData<Double> _float,
        SdkBindingData<String> string,
        SdkBindingData<Boolean> _boolean,
        SdkBindingData<Instant> datetime,
        SdkBindingData<Duration> duration,
        SdkBindingData<Long> a,
        SdkBindingData<Long> b) {
      return new AutoValue_SdkLaunchPlanTest_TestWorkflowInput(
          integer, _float, string, _boolean, datetime, duration, a, b);
    }

    public static class SdkType extends org.flyte.flytekit.SdkType<TestWorkflowInput> {

      private static final String INTEGER = "integer";
      private static final String FLOAT = "_float";
      private static final String STRING = "string";
      private static final String BOOLEAN = "_boolean";
      private static final String DATETIME = "datetime";
      private static final String DURATION = "duration";
      private static final String A = "a";
      private static final String B = "b";

      @Override
      public Map<String, Literal> toLiteralMap(TestWorkflowInput value) {
        return Map.ofEntries(
            Map.entry(INTEGER, Literals.ofInteger(value.integer().get())),
            Map.entry(FLOAT, Literals.ofFloat(value._float().get())),
            Map.entry(STRING, Literals.ofString(value.string().get())),
            Map.entry(BOOLEAN, Literals.ofBoolean(value._boolean().get())),
            Map.entry(DATETIME, Literals.ofDatetime(value.datetime().get())),
            Map.entry(DURATION, Literals.ofDuration(value.duration().get())),
            Map.entry(A, Literals.ofInteger(value.a().get())),
            Map.entry(B, Literals.ofInteger(value.b().get())));
      }

      @Override
      public TestWorkflowInput fromLiteralMap(Map<String, Literal> value) {
        return create(
            SdkBindingDataFactory.of(value.get(INTEGER).scalar().primitive().integerValue()),
            SdkBindingDataFactory.of(value.get(FLOAT).scalar().primitive().floatValue()),
            SdkBindingDataFactory.of(value.get(STRING).scalar().primitive().stringValue()),
            SdkBindingDataFactory.of(value.get(BOOLEAN).scalar().primitive().booleanValue()),
            SdkBindingDataFactory.of(value.get(DATETIME).scalar().primitive().datetime()),
            SdkBindingDataFactory.of(value.get(DURATION).scalar().primitive().duration()),
            SdkBindingDataFactory.of(value.get(A).scalar().primitive().integerValue()),
            SdkBindingDataFactory.of(value.get(B).scalar().primitive().integerValue()));
      }

      @Override
      public TestWorkflowInput promiseFor(String nodeId) {
        return create(
            SdkBindingData.promise(SdkLiteralTypes.integers(), nodeId, INTEGER),
            SdkBindingData.promise(SdkLiteralTypes.floats(), nodeId, FLOAT),
            SdkBindingData.promise(SdkLiteralTypes.strings(), nodeId, STRING),
            SdkBindingData.promise(SdkLiteralTypes.booleans(), nodeId, BOOLEAN),
            SdkBindingData.promise(SdkLiteralTypes.datetimes(), nodeId, DATETIME),
            SdkBindingData.promise(SdkLiteralTypes.durations(), nodeId, DURATION),
            SdkBindingData.promise(SdkLiteralTypes.integers(), nodeId, A),
            SdkBindingData.promise(SdkLiteralTypes.integers(), nodeId, B));
      }

      @Override
      public Map<String, Variable> getVariableMap() {
        return Map.ofEntries(
            Map.entry(INTEGER, Variable.builder().literalType(LiteralTypes.INTEGER).build()),
            Map.entry(FLOAT, Variable.builder().literalType(LiteralTypes.FLOAT).build()),
            Map.entry(STRING, Variable.builder().literalType(LiteralTypes.STRING).build()),
            Map.entry(BOOLEAN, Variable.builder().literalType(LiteralTypes.BOOLEAN).build()),
            Map.entry(DATETIME, Variable.builder().literalType(LiteralTypes.DATETIME).build()),
            Map.entry(DURATION, Variable.builder().literalType(LiteralTypes.DURATION).build()),
            Map.entry(A, Variable.builder().literalType(LiteralTypes.INTEGER).build()),
            Map.entry(B, Variable.builder().literalType(LiteralTypes.INTEGER).build()));
      }

      @Override
      public Map<String, SdkLiteralType<?>> toLiteralTypes() {
        return Map.ofEntries(
            Map.entry(INTEGER, SdkLiteralTypes.integers()),
            Map.entry(FLOAT, SdkLiteralTypes.floats()),
            Map.entry(STRING, SdkLiteralTypes.strings()),
            Map.entry(BOOLEAN, SdkLiteralTypes.booleans()),
            Map.entry(DATETIME, SdkLiteralTypes.datetimes()),
            Map.entry(DURATION, SdkLiteralTypes.durations()),
            Map.entry(A, SdkLiteralTypes.integers()),
            Map.entry(B, SdkLiteralTypes.integers()));
      }

      @Override
      public Map<String, SdkBindingData<?>> toSdkBindingMap(TestWorkflowInput value) {
        return Map.ofEntries(
            Map.entry(INTEGER, value.integer()),
            Map.entry(FLOAT, value._float()),
            Map.entry(STRING, value.string()),
            Map.entry(BOOLEAN, value._boolean()),
            Map.entry(DATETIME, value.datetime()),
            Map.entry(DURATION, value.duration()),
            Map.entry(A, value.a()),
            Map.entry(B, value.b()));
      }
    }
  }

  private static class NoInputsTestWorkflow extends SdkWorkflow<Void, Void> {

    private NoInputsTestWorkflow() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void expand(SdkWorkflowBuilder builder, Void noInput) {
      // no inputs
      return null;
    }
  }
}
