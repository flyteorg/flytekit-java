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

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AutoValueReflectionTest {

  @Test
  void testInterfaceOfClass() {
    Map<String, Variable> interface_ = AutoValueReflection.interfaceOf(AutoValueInput.class);

    assertThat(interface_, equalTo(AutoValueInput.INTERFACE));
  }

  @Test
  void testInterfaceOfVoid() {
    Map<String, Variable> interface_ = AutoValueReflection.interfaceOf(Void.class);

    assertThat(interface_, equalTo(Collections.emptyMap()));
  }

  @ParameterizedTest
  @MethodSource("createBrokenClasses")
  void interfaceOfClassShouldThrowExceptionForInvalidClasses(
      Class<?> brokenClass, String expectedErrMessageFragment) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> AutoValueReflection.interfaceOf(brokenClass));

    assertThat(exception.getMessage(), containsString(expectedErrMessageFragment));
  }

  static Stream<Arguments> createBrokenClasses() {
    return Stream.of(
        Arguments.of(NonAutoValueInput.class, "AutoValue generated class not found"),
        Arguments.of(PrivateConstructorInput.class, "Can't find constructor"),
        Arguments.of(NotAssignableFromInput.class, "is not assignable to"));
  }

  @Test
  void testReadValue() {
    Instant datetime = Instant.ofEpochSecond(12, 34);
    Duration duration = Duration.ofSeconds(56, 78);
    Map<String, Literal> inputMap = new HashMap<>();
    inputMap.put("i", literalOf(Primitive.ofInteger(123L)));
    inputMap.put("f", literalOf(Primitive.ofFloat(123.0)));
    inputMap.put("s", literalOf(Primitive.ofString("123")));
    inputMap.put("b", literalOf(Primitive.ofBoolean(true)));
    inputMap.put("t", literalOf(Primitive.ofDatetime(datetime)));
    inputMap.put("d", literalOf(Primitive.ofDuration(duration)));

    AutoValueInput input = AutoValueReflection.readValue(inputMap, AutoValueInput.class);

    assertThat(input.i(), equalTo(123L));
    assertThat(input.f(), equalTo(123.0));
    assertThat(input.s(), equalTo("123"));
    assertThat(input.b(), equalTo(true));
    assertThat(input.t(), equalTo(datetime));
    assertThat(input.d(), equalTo(duration));
  }

  @Test
  void readValueThrowsExceptionsThrowingConstructors() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AutoValueReflection.readValue(
                    Collections.emptyMap(), ThrowsOnConstructorInput.class));

    assertThat(exception.getMessage(), containsString("Couldn't instantiate class"));
  }

  @ParameterizedTest
  @MethodSource("createInputMaps")
  void readValueShouldThrowExceptionForInvalidInputs(
      Map<String, Literal> inputMap, String expectedErrMessageFragment) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AutoValueReflection.readValue(inputMap, MinimalInput.class));

    assertThat(exception.getMessage(), containsString(expectedErrMessageFragment));
  }

  @Test
  void testToLiteralMap() {
    Map<String, Literal> literalMap =
        AutoValueReflection.toLiteralMap(
            AutoValueInput.create(
                42L, 42.0d, "42", false, Instant.ofEpochSecond(42, 1), Duration.ofSeconds(1, 42)),
            AutoValueInput.class);
    assertThat(literalMap.size(), is(6));
    assertThat(requireNonNull(literalMap.get("i").scalar().primitive()).integer(), is(42L));
    assertThat(requireNonNull(literalMap.get("f").scalar().primitive()).float_(), is(42.0));
    assertThat(requireNonNull(literalMap.get("s").scalar().primitive()).string(), is("42"));
    assertThat(requireNonNull(literalMap.get("b").scalar().primitive()).boolean_(), is(false));
    assertThat(
        requireNonNull(literalMap.get("t").scalar().primitive().datetime()),
        is(Instant.ofEpochSecond(42, 1)));
    assertThat(
        requireNonNull(literalMap.get("d").scalar().primitive()).duration(),
        is(Duration.ofSeconds(1, 42)));
  }

  static Stream<Arguments> createInputMaps() {
    return Stream.of(
        Arguments.of(
            singletonMap("i", literalOf(Primitive.ofString("not a integer"))),
            "is not assignable from"),
        Arguments.of(
            singletonMap("f", literalOf(Primitive.ofString("doesn't contain 'i'"))),
            "is not in inputs"));
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.of(Scalar.of(primitive));
  }

  @AutoValue
  abstract static class AutoValueInput {
    abstract long i();

    abstract double f();

    abstract String s();

    abstract boolean b();

    abstract Instant t();

    abstract Duration d();

    static final Map<String, Variable> INTERFACE = new HashMap<>();

    static {
      INTERFACE.put("i", createVar(SimpleType.INTEGER));
      INTERFACE.put("f", createVar(SimpleType.FLOAT));
      INTERFACE.put("s", createVar(SimpleType.STRING));
      INTERFACE.put("b", createVar(SimpleType.BOOLEAN));
      INTERFACE.put("t", createVar(SimpleType.DATETIME));
      INTERFACE.put("d", createVar(SimpleType.DURATION));
    }

    private static Variable createVar(SimpleType simpleType) {
      return Variable.builder()
          .literalType(LiteralTypes.ofSimpleType(simpleType))
          .description("")
          .build();
    }

    static AutoValueInput create(long i, double f, String s, boolean b, Instant t, Duration d) {
      return new AutoValue_AutoValueReflectionTest_AutoValueInput(i, f, s, b, t, d);
    }
  }

  static class NonAutoValueInput {}

  static class PrivateConstructorInput {}

  static class NotAssignableFromInput {}

  static class ThrowsOnConstructorInput {}

  @AutoValue
  abstract static class MinimalInput {
    abstract long i();
  }
}
