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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Timestamp;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AutoValueReflectionTest {

  @Test
  void testInterfaceOfTypeDescriptor() {
    Map<String, Variable> interface_ =
        AutoValueReflection.interfaceOf(TypeDescriptor.of(AutoValueInput.class));

    assertThat(interface_, equalTo(AutoValueInput.INTERFACE));
  }

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
    long integer = 123L;
    double float_ = 123.0;
    String string = "123";
    boolean boolean_ = true;
    Timestamp datetime = Timestamp.create(123, 456);
    Duration duration = Duration.create(123, 456);
    ImmutableMap<String, Literal> inputMap =
        ImmutableMap.<String, Literal>builder()
            .put("i", literalOf(Primitive.of(integer)))
            .put("f", literalOf(Primitive.of(float_)))
            .put("s", literalOf(Primitive.of(string)))
            .put("b", literalOf(Primitive.of(boolean_)))
            .put("t", literalOf(Primitive.of(datetime)))
            .put("d", literalOf(Primitive.of(duration)))
            .build();

    AutoValueInput input = AutoValueReflection.readValue(inputMap, AutoValueInput.class);

    assertThat(input.i(), equalTo(integer));
    assertThat(input.f(), equalTo(float_));
    assertThat(input.s(), equalTo(string));
    assertThat(input.b(), equalTo(boolean_));
    assertThat(input.t(), equalTo(datetime));
    assertThat(input.d(), equalTo(duration));
  }

  @Test
  void readValueThrowsExceptionsThrowingConstructors() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AutoValueReflection.readValue(ImmutableMap.of(), ThrowsOnConstructorInput.class));

    assertThat(exception.getMessage(), containsString("Couldn't instantiate class"));
  }

  @ParameterizedTest
  @MethodSource("createInputMaps")
  void readValueShouldThrowExceptionForInvalidInputs(
      ImmutableMap<String, Literal> inputMap, String expectedErrMessageFragment) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AutoValueReflection.readValue(inputMap, MinimalInput.class));

    assertThat(exception.getMessage(), containsString(expectedErrMessageFragment));
  }

  static Stream<Arguments> createInputMaps() {
    return Stream.of(
        Arguments.of(
            ImmutableMap.of("i", literalOf(Primitive.of("not a integer"))),
            "is not assignable from"),
        Arguments.of(
            ImmutableMap.of("f", literalOf(Primitive.of("doesn't contain 'i'"))),
            "is not in inputs"));
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.of(Scalar.create(primitive));
  }

  @AutoValue
  abstract static class AutoValueInput {
    abstract long i();

    abstract double f();

    abstract String s();

    abstract boolean b();

    abstract Timestamp t();

    abstract Duration d();

    static final ImmutableMap<String, Variable> INTERFACE =
        ImmutableMap.<String, Variable>builder()
            .put("i", Variable.create(LiteralType.create(SimpleType.INTEGER), ""))
            .put("f", Variable.create(LiteralType.create(SimpleType.FLOAT), ""))
            .put("s", Variable.create(LiteralType.create(SimpleType.STRING), ""))
            .put("b", Variable.create(LiteralType.create(SimpleType.BOOLEAN), ""))
            .put("t", Variable.create(LiteralType.create(SimpleType.DATETIME), ""))
            .put("d", Variable.create(LiteralType.create(SimpleType.DURATION), ""))
            .build();
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
