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
package org.flyte.flytekit.jackson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType.Kind;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JacksonSdkLiteralTypeTest {

  private JacksonSdkLiteralType<SomeType> type;

  @BeforeEach
  void setUp() {
    type = JacksonSdkLiteralType.of(SomeType.class);
  }

  @Test
  void literalTypeShouldBeStruct() {
    var literalType = type.getLiteralType();

    assertThat(literalType.getKind(), equalTo(Kind.SIMPLE_TYPE));
    assertThat(literalType.simpleType(), equalTo(SimpleType.STRUCT));
  }

  @Test
  void shouldConvertValuesToLiteral() {
    var value =
        SomeType.create(1, 2.0, "3", true, null, List.of("4", "5", "6"), EmbeddedType.create(7, 8));
    var expected =
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
                                    "b", Value.ofNumberValue(8))))))));

    var literal = type.toLiteral(value);

    assertThat(literal, equalTo(expected));
  }

  @Test
  void shouldConvertNullValuesToNullLiteral() {
    var literal = type.toLiteral(null);

    assertThat(literal, nullValue());
  }

  @Test
  void shouldConvertLiteralToValue() {
    var literal =
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
                                    "b", Value.ofNumberValue(8))))))));
    var expected =
        SomeType.create(1, 2.0, "3", true, null, List.of("4", "5", "6"), EmbeddedType.create(7, 8));

    var value = type.fromLiteral(literal);

    assertThat(value, equalTo(expected));
  }

  @Test
  void shouldConvertNullLiteralToNullValue() {
    var value = type.fromLiteral(null);

    assertThat(value, nullValue());
  }

  @Test
  void shouldConvertValuesToBindingData() {
    var value =
        SomeType.create(1, 2.0, "3", true, null, List.of("4", "5", "6"), EmbeddedType.create(7, 8));
    var expected =
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
                                    "b", Value.ofNumberValue(8))))))));

    var bindingData = type.toBindingData(value);

    assertThat(bindingData, equalTo(expected));
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
  void shouldThrowExceptionForTypes2(Class<?> type) {
    var ex = assertThrows(IllegalArgumentException.class, () -> JacksonSdkLiteralType.of(type));

    assertThat(
        ex.getMessage(),
        equalTo(
            String.format(
                "Class [%s] not compatible with JacksonSdkLiteralType. Use SdkLiteralType.of instead",
                type.getName())));
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
}
