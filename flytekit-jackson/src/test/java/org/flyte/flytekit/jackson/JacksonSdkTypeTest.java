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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.api.v1.LiteralType.ofSimpleType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.BlobMetadata;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkType;
import org.junit.jupiter.api.Test;

public class JacksonSdkTypeTest {

  static final BlobType BLOB_TYPE =
      BlobType.builder().format("").dimensionality(BlobType.BlobDimensionality.SINGLE).build();

  @Test
  public void testVariableMap() {
    Map<String, Variable> expected = new HashMap<>();
    expected.put("i", createVar(SimpleType.INTEGER));
    expected.put("f", createVar(SimpleType.FLOAT));
    expected.put("s", createVar(SimpleType.STRING));
    expected.put("b", createVar(SimpleType.BOOLEAN));
    expected.put("t", createVar(SimpleType.DATETIME));
    expected.put("d", createVar(SimpleType.DURATION));
    expected.put("blob", createVar(LiteralType.ofBlobType(BLOB_TYPE)));
    expected.put("l", createVar(LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING))));
    expected.put("m", createVar(LiteralType.ofMapValueType(ofSimpleType(SimpleType.STRING))));

    assertEquals(expected, JacksonSdkType.of(AutoValueInput.class).getVariableMap());
  }

  @Test
  void testFromLiteralMap() {
    Instant datetime = Instant.ofEpochSecond(12, 34);
    Duration duration = Duration.ofSeconds(56, 78);
    Blob blob =
        Blob.builder()
            .metadata(BlobMetadata.builder().type(BLOB_TYPE).build())
            .uri("file://test")
            .build();
    Map<String, Literal> literalMap = new HashMap<>();
    literalMap.put("i", literalOf(Primitive.ofIntegerValue(123L)));
    literalMap.put("f", literalOf(Primitive.ofFloatValue(123.0)));
    literalMap.put("s", literalOf(Primitive.ofStringValue("123")));
    literalMap.put("b", literalOf(Primitive.ofBooleanValue(true)));
    literalMap.put("t", literalOf(Primitive.ofDatetime(datetime)));
    literalMap.put("d", literalOf(Primitive.ofDuration(duration)));
    literalMap.put("blob", literalOf(blob));
    literalMap.put(
        "l", Literal.ofCollection(singletonList(literalOf(Primitive.ofStringValue("123")))));
    literalMap.put(
        "m", Literal.ofMap(singletonMap("marco", literalOf(Primitive.ofStringValue("polo")))));

    AutoValueInput input = JacksonSdkType.of(AutoValueInput.class).fromLiteralMap(literalMap);

    assertThat(
        input,
        equalTo(
            AutoValueInput.create(
                /* i= */ 123L,
                /* f= */ 123.0,
                /* s= */ "123",
                /* b= */ true,
                /* t= */ datetime,
                /* d= */ duration,
                /* blob= */ blob,
                /* l= */ singletonList("123"),
                /* m= */ singletonMap("marco", "polo"))));
  }

  @Test
  void testToLiteralMap() {
    Blob blob =
        Blob.builder()
            .metadata(BlobMetadata.builder().type(BLOB_TYPE).build())
            .uri("file://test")
            .build();
    Map<String, Literal> literalMap =
        JacksonSdkType.of(AutoValueInput.class)
            .toLiteralMap(
                AutoValueInput.create(
                    /* i= */ 42L,
                    /* f= */ 42.0d,
                    /* s= */ "42",
                    /* b= */ false,
                    /* t= */ Instant.ofEpochSecond(42, 1),
                    /* d= */ Duration.ofSeconds(1, 42),
                    /* blob= */ blob,
                    /* l= */ singletonList("foo"),
                    /* m= */ singletonMap("marco", "polo")));

    Map<String, Literal> expected = new HashMap<>();
    expected.put("i", literalOf(Primitive.ofIntegerValue(42L)));
    expected.put("f", literalOf(Primitive.ofFloatValue(42.0d)));
    expected.put("s", literalOf(Primitive.ofStringValue("42")));
    expected.put("b", literalOf(Primitive.ofBooleanValue(false)));
    expected.put("t", literalOf(Primitive.ofDatetime(Instant.ofEpochSecond(42, 1))));
    expected.put("d", literalOf(Primitive.ofDuration(Duration.ofSeconds(1, 42))));
    expected.put(
        "l", Literal.ofCollection(singletonList(literalOf(Primitive.ofStringValue("foo")))));
    expected.put(
        "m", Literal.ofMap(singletonMap("marco", literalOf(Primitive.ofStringValue("polo")))));
    expected.put("blob", literalOf(blob));

    assertThat(literalMap, equalTo(expected));
  }

  @Test
  public void testPojoToLiteralMap() {
    PojoInput input = new PojoInput();
    input.a = 42;

    Map<String, Literal> literalMap = JacksonSdkType.of(PojoInput.class).toLiteralMap(input);

    assertThat(literalMap, equalTo(singletonMap("a", literalOf(Primitive.ofIntegerValue(42)))));
  }

  @Test
  public void testPojoFromLiteralMap() {
    PojoInput expected = new PojoInput();
    expected.a = 42;

    PojoInput pojoInput =
        JacksonSdkType.of(PojoInput.class)
            .fromLiteralMap(singletonMap("a", literalOf(Primitive.ofIntegerValue(42))));

    assertThat(pojoInput, equalTo(expected));
  }

  @Test
  public void testPojoVariableMap() {
    Variable expected =
        Variable.builder().description("").literalType(LiteralTypes.INTEGER).build();

    Map<String, Variable> variableMap = JacksonSdkType.of(PojoInput.class).getVariableMap();

    assertThat(variableMap, equalTo(singletonMap("a", expected)));
  }

  @Test
  public void testStructRoundtrip() {
    StructInput input =
        StructInput.create(
            StructValueInput.create(
                /* stringValue= */ "nested-string",
                /* boolValue= */ false,
                /* listValue= */ Arrays.asList(1, 2, 3),
                /* structValue= */ StructValueInput.create(
                    /* stringValue= */ "nested-nested-string",
                    /* boolValue= */ true,
                    /* listValue= */ Arrays.asList(4, 5, 6),
                    /* structValue= */ null,
                    /* numberValue= */ 1337.0),
                /* numberValue= */ 42.0));

    SdkType<StructInput> sdkType = JacksonSdkType.of(StructInput.class);

    assertThat(sdkType.fromLiteralMap(sdkType.toLiteralMap(input)), equalTo(input));
  }

  @Test
  public void testConverterToLiteralMap() {
    InputWithCustomType input = InputWithCustomType.create(CustomType.ONE, CustomEnum.TWO);
    Map<String, Literal> expected = new HashMap<>();
    expected.put("customType", literalOf(Primitive.ofStringValue("ONE")));
    expected.put("customEnum", literalOf(Primitive.ofStringValue("TWO")));

    Map<String, Literal> literalMap =
        JacksonSdkType.of(InputWithCustomType.class).toLiteralMap(input);

    assertThat(literalMap, equalTo(expected));
  }

  @Test
  public void testConverterFromLiteralMap() {
    InputWithCustomType expected = InputWithCustomType.create(CustomType.TWO, CustomEnum.ONE);
    Map<String, Literal> literalMap = new HashMap<>();
    literalMap.put("customType", literalOf(Primitive.ofStringValue("TWO")));
    literalMap.put("customEnum", literalOf(Primitive.ofStringValue("ONE")));

    InputWithCustomType output =
        JacksonSdkType.of(InputWithCustomType.class).fromLiteralMap(literalMap);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testConverterVariableMap() {
    Map<String, Variable> expected = new HashMap<>();
    expected.put(
        "customType", Variable.builder().description("").literalType(LiteralTypes.STRING).build());
    expected.put(
        "customEnum", Variable.builder().description("").literalType(LiteralTypes.STRING).build());

    Map<String, Variable> variableMap =
        JacksonSdkType.of(InputWithCustomType.class).getVariableMap();

    assertThat(variableMap, equalTo(expected));
  }

  @Test
  void testUnknownSerializer() {
    // Serialization doesn't work because Jackson doesn't recognize empty classes as
    // Java beans good thing that exception is thrown when constructing JacksonSdkType
    // and not at the moment when we need to serialize.
    //
    // If class doesn't have creator, we can serialize, but we can't deserialize it.
    // It isn't checked at the moment, because we don't know if JacksonSdkType is constructed
    // for input (that needs deserialization) or output (that doesn't).
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> JacksonSdkType.of(Unannotated.class));

    assertThat(
        e.getMessage(),
        equalTo(
            "Failed to find serializer for [org.flyte.flytekit.jackson.JacksonSdkTypeTest$Unannotated]"));
    assertThat(
        e.getCause().getMessage(),
        equalTo(
            "No serializer found for class org.flyte.flytekit.jackson.JacksonSdkTypeTest$Unannotated and no properties discovered to create BeanSerializer"));
  }

  public static class Unannotated {}

  @AutoValue
  public abstract static class AutoValueInput {
    public abstract long i();

    public abstract double f();

    public abstract String s();

    public abstract boolean b();

    public abstract Instant t();

    public abstract Duration d();

    public abstract Blob blob();

    public abstract List<String> l();

    public abstract Map<String, String> m();

    public static AutoValueInput create(
        long i,
        double f,
        String s,
        boolean b,
        Instant t,
        Duration d,
        Blob blob,
        List<String> l,
        Map<String, String> m) {
      return new AutoValue_JacksonSdkTypeTest_AutoValueInput(i, f, s, b, t, d, blob, l, m);
    }
  }

  @AutoValue
  public abstract static class StructInput {
    public abstract StructValueInput structValue();

    public static StructInput create(StructValueInput structValue) {
      return new AutoValue_JacksonSdkTypeTest_StructInput(structValue);
    }
  }

  @AutoValue
  public abstract static class StructValueInput {
    public abstract String stringValue();

    public abstract boolean boolValue();

    public abstract List<Integer> listValue();

    @Nullable
    public abstract StructValueInput structValue();

    public abstract Double numberValue();

    public static StructValueInput create(
        String stringValue,
        boolean boolValue,
        List<Integer> listValue,
        StructValueInput structValue,
        Double numberValue) {
      return new AutoValue_JacksonSdkTypeTest_StructValueInput(
          stringValue, boolValue, listValue, structValue, numberValue);
    }
  }

  public static final class PojoInput {
    public long a;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PojoInput pojoInput = (PojoInput) o;
      return a == pojoInput.a;
    }

    @Override
    public int hashCode() {
      return Objects.hash(a);
    }
  }

  @AutoValue
  public abstract static class InputWithCustomType {
    public abstract CustomType customType();

    public abstract CustomEnum customEnum();

    public static InputWithCustomType create(CustomType customType, CustomEnum customEnum) {
      return new AutoValue_JacksonSdkTypeTest_InputWithCustomType(customType, customEnum);
    }
  }

  @JsonSerialize(converter = CustomType.ToString.class)
  @JsonDeserialize(converter = CustomType.FromString.class)
  public static final class CustomType {
    private final int ordinal;

    private CustomType(int ordinal) {
      this.ordinal = ordinal;
    }

    public static final CustomType ONE = new CustomType(1);
    public static final CustomType TWO = new CustomType(2);
    public static final CustomType UNKNOWN = new CustomType(-1);

    public static class ToString extends StdConverter<CustomType, String> {
      @Override
      public String convert(CustomType value) {
        if (value == ONE) {
          return "ONE";
        } else if (value == TWO) {
          return "TWO";
        } else {
          return "UNKNOWN";
        }
      }
    }

    public static class FromString extends StdConverter<String, CustomType> {
      @Override
      public CustomType convert(String value) {
        if (value.equals("ONE")) {
          return ONE;
        } else if (value.equals("TWO")) {
          return TWO;
        } else {
          return UNKNOWN;
        }
      }
    }

    @Override
    public String toString() {
      return "CustomType{ordinal=" + ordinal + "}";
    }
  }

  public enum CustomEnum {
    ONE,
    TWO
  }

  private static Variable createVar(SimpleType simpleType) {
    return createVar(ofSimpleType(simpleType));
  }

  private static Variable createVar(LiteralType literalType) {
    return Variable.builder().literalType(literalType).description("").build();
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  private static Literal literalOf(Blob blob) {
    return Literal.ofScalar(Scalar.ofBlob(blob));
  }
}
