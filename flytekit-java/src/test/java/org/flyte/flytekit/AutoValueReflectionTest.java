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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.flyte.api.v1.LiteralType.ofSimpleType;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralBoolean;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralDatetime;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralDuration;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralFloat;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralInteger;
import static org.flyte.flytekit.LiteralMatchers.matchesLiteralString;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
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
    inputMap.put("l", Literal.ofCollection(singletonList(literalOf(Primitive.ofString("123")))));
    inputMap.put("m", Literal.ofMap(singletonMap("marco", literalOf(Primitive.ofString("polo")))));

    AutoValueInput input = AutoValueReflection.readValue(inputMap, AutoValueInput.class);

    assertThat(input.i(), equalTo(123L));
    assertThat(input.f(), equalTo(123.0));
    assertThat(input.s(), equalTo("123"));
    assertThat(input.b(), equalTo(true));
    assertThat(input.t(), equalTo(datetime));
    assertThat(input.d(), equalTo(duration));
    assertThat(input.l(), equalTo(singletonList("123")));
    assertThat(input.m(), equalTo(singletonMap("marco", "polo")));
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
                42L,
                42.0d,
                "42",
                false,
                Instant.ofEpochSecond(42, 1),
                Duration.ofSeconds(1, 42),
                singletonList("foo"),
                singletonMap("marco", "polo")),
            AutoValueInput.class);
    assertThat(literalMap.size(), is(8));
    assertThat(literalMap.get("i"), matchesLiteralInteger(is(42L)));
    assertThat(literalMap.get("f"), matchesLiteralFloat(is(42.0)));
    assertThat(literalMap.get("s"), matchesLiteralString(is("42")));
    assertThat(literalMap.get("b"), matchesLiteralBoolean(is(false)));
    assertThat(literalMap.get("t"), matchesLiteralDatetime(is(Instant.ofEpochSecond(42, 1))));
    assertThat(literalMap.get("d"), matchesLiteralDuration(is(Duration.ofSeconds(1, 42))));
    requireNonNull(literalMap.get("l").collection())
        .forEach(elem -> assertThat(elem, matchesLiteralString(is("foo"))));
    requireNonNull(literalMap.get("m").map())
        .forEach(
            (name, value) -> {
              assertThat(name, is("marco"));
              assertThat(value, matchesLiteralString(is("polo")));
            });
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
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  @AutoValue
  abstract static class AutoValueInput {
    abstract long i();

    abstract double f();

    abstract String s();

    abstract boolean b();

    abstract Instant t();

    abstract Duration d();

    abstract List<String> l();

    abstract Map<String, String> m();

    static final Map<String, Variable> INTERFACE = new HashMap<>();

    static {
      INTERFACE.put("i", createVar(SimpleType.INTEGER));
      INTERFACE.put("f", createVar(SimpleType.FLOAT));
      INTERFACE.put("s", createVar(SimpleType.STRING));
      INTERFACE.put("b", createVar(SimpleType.BOOLEAN));
      INTERFACE.put("t", createVar(SimpleType.DATETIME));
      INTERFACE.put("d", createVar(SimpleType.DURATION));
      INTERFACE.put("l", createVar(LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING))));
      INTERFACE.put("m", createVar(LiteralType.ofMapValueType(ofSimpleType(SimpleType.STRING))));
    }

    private static Variable createVar(SimpleType simpleType) {
      return createVar(ofSimpleType(simpleType));
    }

    private static Variable createVar(LiteralType literalType) {
      return Variable.builder().literalType(literalType).description("").build();
    }

    public static AutoValueInput create(
        long i,
        double f,
        String s,
        boolean b,
        Instant t,
        Duration d,
        List<String> l,
        Map<String, String> m) {
      return new AutoValue_AutoValueReflectionTest_AutoValueInput(i, f, s, b, t, d, l, m);
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
