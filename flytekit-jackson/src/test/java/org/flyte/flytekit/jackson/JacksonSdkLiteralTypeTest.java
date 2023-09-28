/*
 * Copyright 2023 Flyte Authors
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
package org.flyte.flytekit.jackson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType.Kind;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class JacksonSdkLiteralTypeTest {

  @Test
  void literalTypeShouldBeStruct() {
    var sdkLiteralType = JacksonSdkLiteralType.of(SomeType.class);

    var literalType = sdkLiteralType.getLiteralType();

    assertThat(literalType.getKind(), equalTo(Kind.SIMPLE_TYPE));
    assertThat(literalType.simpleType(), equalTo(SimpleType.STRUCT));
  }

  @ParameterizedTest
  @MethodSource("typeValueLiteralProvider")
  <T> void shouldConvertValuesToLiteral(Class<T> type, T value, Literal expected) {
    var sdkLiteralType = JacksonSdkLiteralType.of(type);

    var literal = sdkLiteralType.toLiteral(value);

    assertThat(literal, equalTo(expected));
  }

  @ParameterizedTest
  @MethodSource("typeValueLiteralProvider")
  <T> void shouldConvertLiteralToValue(Class<T> type, T expected, Literal literal) {
    var sdkLiteralType = JacksonSdkLiteralType.of(type);

    var value = sdkLiteralType.fromLiteral(literal);

    assertThat(value, equalTo(expected));
  }

  public static Stream<Arguments> typeValueLiteralProvider() {
    return Stream.of(
        arguments(SomeType.class, null, null),
        arguments(
            SomeType.class,
            SomeType.create(
                1, 2.0, "3", true, null, List.of("4", "5", "6"), EmbeddedType.create(7, 8)),
            Literal.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "i",
                            Value.ofNumberValue(1),
                            "f",
                            Value.ofNumberValue(2.0),
                            "s",
                            Value.ofStringValue("3"),
                            "b",
                            Value.ofBoolValue(true),
                            "null_",
                            Value.ofNullValue(),
                            "list",
                            Value.ofListValue(
                                List.of(
                                    Value.ofStringValue("4"),
                                    Value.ofStringValue("5"),
                                    Value.ofStringValue("6"))),
                            "subTest",
                            Value.ofStructValue(
                                Struct.of(
                                    Map.of(
                                        "a", Value.ofNumberValue(7),
                                        "b", Value.ofNumberValue(8))))))))),
        arguments(
            TypeWithMap.class,
            TypeWithMap.create(Map.of("x", 1L, "y", 2L)),
            Literal.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "a",
                            Value.ofStructValue(
                                Struct.of(
                                    Map.of(
                                        "x", Value.ofNumberValue(1L),
                                        "y", Value.ofNumberValue(2L))))))))),
        arguments(
            TypeWithUnsupportedMap.class,
            TypeWithUnsupportedMap.create(Map.of(true, "a")),
            Literal.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "a",
                            Value.ofStructValue(
                                Struct.of(Map.of("true", Value.ofStringValue("a"))))))))));
  }

  @ParameterizedTest
  @MethodSource("typeValueBindingProvider")
  <T> void shouldConvertValuesToBindingData(Class<T> type, T value, BindingData expected) {
    var sdkLiteralType = JacksonSdkLiteralType.of(type);

    var bindingData = sdkLiteralType.toBindingData(value);

    assertThat(bindingData, equalTo(expected));
  }

  public static Stream<Arguments> typeValueBindingProvider() {
    return Stream.of(
        arguments(SomeType.class, null, null),
        arguments(
            SomeType.class,
            SomeType.create(
                1, 2.0, "3", true, null, List.of("4", "5", "6"), EmbeddedType.create(7, 8)),
            BindingData.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "i",
                            Value.ofNumberValue(1),
                            "f",
                            Value.ofNumberValue(2.0),
                            "s",
                            Value.ofStringValue("3"),
                            "b",
                            Value.ofBoolValue(true),
                            "null_",
                            Value.ofNullValue(),
                            "list",
                            Value.ofListValue(
                                List.of(
                                    Value.ofStringValue("4"),
                                    Value.ofStringValue("5"),
                                    Value.ofStringValue("6"))),
                            "subTest",
                            Value.ofStructValue(
                                Struct.of(
                                    Map.of(
                                        "a", Value.ofNumberValue(7),
                                        "b", Value.ofNumberValue(8))))))))),
        arguments(
            TypeWithMap.class,
            TypeWithMap.create(Map.of("a", 1L, "b", 2L)),
            BindingData.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "a",
                            Value.ofStructValue(
                                Struct.of(
                                    Map.of(
                                        "a", Value.ofNumberValue(1L),
                                        "b", Value.ofNumberValue(2L))))))))),
        // technically maps should have string keys, however it works as long as the struct key
        // value is convertible to the map key type
        arguments(
            TypeWithUnsupportedMap.class,
            TypeWithUnsupportedMap.create(Map.of(true, "a")),
            BindingData.ofScalar(
                Scalar.ofGeneric(
                    Struct.of(
                        Map.of(
                            "a",
                            Value.ofStructValue(
                                Struct.of(Map.of("true", Value.ofStringValue("a"))))))))));
  }

  @ParameterizedTest
  @ValueSource(classes = {TypeWithNoProperties.class, TypeWithPrivateConstructor.class})
  void shouldThrowExceptionForTypesWithNoJacksonSerializers(Class<?> type) {
    var ex = assertThrows(IllegalArgumentException.class, () -> JacksonSdkLiteralType.of(type));

    assertThat(
        ex.getMessage(),
        equalTo(String.format("Failed to find serializer for [%s]", type.getName())));
  }

  @ParameterizedTest
  @ValueSource(
      classes = {
        Long.class,
        Double.class,
        String.class,
        Boolean.class,
        Duration.class,
        Instant.class,
        List.class,
        Map.class
      })
  void shouldThrowExceptionForNonBeanTypes(Class<?> type) {
    var ex = assertThrows(IllegalArgumentException.class, () -> JacksonSdkLiteralType.of(type));

    assertThat(
        ex.getMessage(),
        equalTo(
            String.format(
                "Class [%s] not compatible with JacksonSdkLiteralType. Use SdkLiteralType.of instead",
                type.getName())));
  }

  @Test
  void shouldThrowExceptionForTypes2() {
    var sdkLiteralType = JacksonSdkLiteralType.of(TypeWithUnsupportedMap.class);
    Literal literal =
        Literal.ofScalar(
            Scalar.ofGeneric(
                Struct.of(
                    Map.of(
                        "a",
                        Value.ofStructValue(
                            Struct.of(Map.of("not-a-boolean", Value.ofStringValue("a"))))))));

    var ex = assertThrows(RuntimeException.class, () -> sdkLiteralType.fromLiteral(literal));

    assertThat(
        ex.getMessage(),
        equalTo(
            String.format(
                "fromLiteral failed for [%s]: %s",
                TypeWithUnsupportedMap.class.getName(), literal)));
  }

  @AutoValue
  abstract static class SomeType {

    abstract long i();

    abstract double f();

    abstract String s();

    abstract boolean b();

    @Nullable
    abstract String null_();

    abstract List<String> list();

    abstract EmbeddedType subTest();

    public static SomeType create(
        long i,
        double f,
        String s,
        boolean b,
        String null_,
        List<String> list,
        EmbeddedType subTest) {
      return new AutoValue_JacksonSdkLiteralTypeTest_SomeType(i, f, s, b, null_, list, subTest);
    }
  }

  @AutoValue
  abstract static class EmbeddedType {
    abstract long a();

    abstract long b();

    public static EmbeddedType create(long a, long b) {
      return new AutoValue_JacksonSdkLiteralTypeTest_EmbeddedType(a, b);
    }
  }

  static class TypeWithPrivateConstructor {
    private TypeWithPrivateConstructor() {
      // private constructor prevents Jackson from instantiating objects
    }

    int getFoo() {
      return 0;
    }
  }

  static class TypeWithNoProperties {}

  @AutoValue
  abstract static class TypeWithMap {
    abstract Map<String, Long> a();

    public static TypeWithMap create(Map<String, Long> a) {
      return new AutoValue_JacksonSdkLiteralTypeTest_TypeWithMap(a);
    }
  }

  @AutoValue
  abstract static class TypeWithUnsupportedMap {
    abstract Map<Boolean, String> a();

    public static TypeWithUnsupportedMap create(Map<Boolean, String> a) {
      return new AutoValue_JacksonSdkLiteralTypeTest_TypeWithUnsupportedMap(a);
    }
  }
}
