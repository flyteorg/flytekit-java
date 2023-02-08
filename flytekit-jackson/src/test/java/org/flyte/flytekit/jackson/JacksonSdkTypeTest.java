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

import static org.flyte.api.v1.LiteralType.ofSimpleType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDatas;
import org.flyte.flytekit.SdkLiteralTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JacksonSdkTypeTest {

  @SuppressWarnings("UnusedVariable")
  static final BlobType BLOB_TYPE =
      BlobType.builder().format("").dimensionality(BlobType.BlobDimensionality.SINGLE).build();

  public static AutoValueInput createAutoValueInput(
      long i,
      double f,
      String s,
      boolean b,
      Instant t,
      Duration d,
      // Blob blob,
      List<String> l,
      Map<String, String> m,
      List<List<String>> ll,
      List<Map<String, String>> lm,
      Map<String, List<String>> ml,
      Map<String, Map<String, String>> mm) {
    return AutoValueInput.create(
        SdkBindingDatas.ofInteger(i),
        SdkBindingDatas.ofFloat(f),
        SdkBindingDatas.ofString(s),
        SdkBindingDatas.ofBoolean(b),
        SdkBindingDatas.ofDatetime(t),
        SdkBindingDatas.ofDuration(d),
        SdkBindingDatas.ofStringCollection(l),
        SdkBindingDatas.ofStringMap(m),
        SdkBindingDatas.ofCollection(SdkLiteralTypes.collections(SdkLiteralTypes.strings()), ll),
        SdkBindingDatas.ofCollection(SdkLiteralTypes.maps(SdkLiteralTypes.strings()), lm),
        SdkBindingDatas.ofMap(SdkLiteralTypes.collections(SdkLiteralTypes.strings()), ml),
        SdkBindingDatas.ofMap(SdkLiteralTypes.maps(SdkLiteralTypes.strings()), mm));
  }

  @Test
  public void testVariableMap() {
    assertThat(
        JacksonSdkType.of(AutoValueInput.class).getVariableMap(),
        allOf(
            List.of(
                hasEntry("i", createVar(SimpleType.INTEGER, "input i")),
                hasEntry("f", createVar(SimpleType.FLOAT)),
                hasEntry("s", createVar(SimpleType.STRING)),
                hasEntry("b", createVar(SimpleType.BOOLEAN)),
                hasEntry("t", createVar(SimpleType.DATETIME)),
                hasEntry("d", createVar(SimpleType.DURATION)),
                // hasEntry("blob", createVar(LiteralType.ofBlobType(BLOB_TYPE))),
                hasEntry(
                    "l", createVar(LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING)))),
                hasEntry(
                    "m", createVar(LiteralType.ofMapValueType(ofSimpleType(SimpleType.STRING)))),
                hasEntry(
                    "ll",
                    createVar(
                        LiteralType.ofCollectionType(
                            LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING))))),
                hasEntry(
                    "ml",
                    createVar(
                        LiteralType.ofMapValueType(
                            LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING))))))));
  }

  @Test
  void testFromLiteralMap() {
    Instant datetime = Instant.ofEpochSecond(12, 34);
    Duration duration = Duration.ofSeconds(56, 78);
    //    Blob blob =
    //        Blob.builder()
    //            .metadata(BlobMetadata.builder().type(BLOB_TYPE).build())
    //            .uri("file://test")
    //            .build();
    Map<String, Literal> literalMap = new HashMap<>();
    literalMap.put("i", literalOf(Primitive.ofIntegerValue(123L)));
    literalMap.put("f", literalOf(Primitive.ofFloatValue(123.0)));
    literalMap.put("s", literalOf(Primitive.ofStringValue("123")));
    literalMap.put("b", literalOf(Primitive.ofBooleanValue(true)));
    literalMap.put("t", literalOf(Primitive.ofDatetime(datetime)));
    literalMap.put("d", literalOf(Primitive.ofDuration(duration)));
    // literalMap.put("blob", literalOf(blob));
    literalMap.put("l", Literal.ofCollection(List.of(literalOf(Primitive.ofStringValue("123")))));
    literalMap.put("m", Literal.ofMap(Map.of("marco", literalOf(Primitive.ofStringValue("polo")))));
    literalMap.put(
        "ll",
        Literal.ofCollection(
            List.of(
                Literal.ofCollection(List.of(stringLiteralOf("foo"), stringLiteralOf("bar"))),
                Literal.ofCollection(
                    List.of(stringLiteralOf("a"), stringLiteralOf("b"), stringLiteralOf("c"))))));
    literalMap.put(
        "lm",
        Literal.ofCollection(
            List.of(
                Literal.ofMap(Map.of("A", stringLiteralOf("a"), "B", stringLiteralOf("b"))),
                Literal.ofMap(Map.of("a", stringLiteralOf("A"), "b", stringLiteralOf("B"))))));
    literalMap.put(
        "ml",
        Literal.ofMap(
            Map.of(
                "frodo",
                Literal.ofCollection(
                    List.of(stringLiteralOf("baggins"), stringLiteralOf("bolson"))))));
    literalMap.put(
        "mm",
        Literal.ofMap(
            Map.of(
                "math",
                    Literal.ofMap(
                        Map.of("pi", stringLiteralOf("3.14"), "e", stringLiteralOf("2.72"))),
                "pokemon", Literal.ofMap(Map.of("ash", stringLiteralOf("pikachu"))))));

    AutoValueInput input = JacksonSdkType.of(AutoValueInput.class).fromLiteralMap(literalMap);

    assertThat(
        input,
        equalTo(
            createAutoValueInput(
                /* i= */ 123L,
                /* f= */ 123.0,
                /* s= */ "123",
                /* b= */ true,
                /* t= */ datetime,
                /* d= */ duration,
                /// * blob= */ blob,
                /* l= */ List.of("123"),
                /* m= */ Map.of("marco", "polo"),
                /* ll= */ List.of(List.of("foo", "bar"), List.of("a", "b", "c")),
                /* lm= */ List.of(Map.of("A", "a", "B", "b"), Map.of("a", "A", "b", "B")),
                /* ml= */ Map.of("frodo", List.of("baggins", "bolson")),
                /* mm= */ Map.of(
                    "math",
                    Map.of("pi", "3.14", "e", "2.72"),
                    "pokemon",
                    Map.of("ash", "pikachu")))));
  }

  private static Literal stringLiteralOf(String string) {
    return literalOf(Primitive.ofStringValue(string));
  }

  @Test
  void testToLiteralMap() {
    //    Blob blob =
    //        Blob.builder()
    //            .metadata(BlobMetadata.builder().type(BLOB_TYPE).build())
    //            .uri("file://test")
    //            .build();
    Map<String, Literal> literalMap =
        JacksonSdkType.of(AutoValueInput.class)
            .toLiteralMap(
                createAutoValueInput(
                    /* i= */ 42L,
                    /* f= */ 42.0d,
                    /* s= */ "42",
                    /* b= */ false,
                    /* t= */ Instant.ofEpochSecond(42, 1),
                    /* d= */ Duration.ofSeconds(1, 42),
                    /// * blob= */ blob,
                    /* l= */ List.of("foo"),
                    /* m= */ Map.of("marco", "polo"),
                    /* ll= */ List.of(List.of("foo", "bar"), List.of("a", "b", "c")),
                    /* lm= */ List.of(Map.of("A", "a", "B", "b"), Map.of("a", "A", "b", "B")),
                    /* ml= */ Map.of("frodo", List.of("baggins", "bolson")),
                    /* mm= */ Map.of(
                        "math",
                        Map.of("pi", "3.14", "e", "2.72"),
                        "pokemon",
                        Map.of("ash", "pikachu"))));

    assertThat(
        literalMap,
        allOf(
            List.of(
                hasEntry("i", literalOf(Primitive.ofIntegerValue(42L))),
                hasEntry("f", literalOf(Primitive.ofFloatValue(42.0d))),
                hasEntry("s", literalOf(Primitive.ofStringValue("42"))),
                hasEntry("b", literalOf(Primitive.ofBooleanValue(false))),
                hasEntry("t", literalOf(Primitive.ofDatetime(Instant.ofEpochSecond(42, 1)))),
                hasEntry("d", literalOf(Primitive.ofDuration(Duration.ofSeconds(1, 42)))),
                hasEntry(
                    "l", Literal.ofCollection(List.of(literalOf(Primitive.ofStringValue("foo"))))),
                hasEntry(
                    "m",
                    Literal.ofMap(Map.of("marco", literalOf(Primitive.ofStringValue("polo"))))),
                hasEntry(
                    "ll",
                    Literal.ofCollection(
                        List.of(
                            Literal.ofCollection(
                                List.of(stringLiteralOf("foo"), stringLiteralOf("bar"))),
                            Literal.ofCollection(
                                List.of(
                                    stringLiteralOf("a"),
                                    stringLiteralOf("b"),
                                    stringLiteralOf("c")))))),
                hasEntry(
                    "lm",
                    Literal.ofCollection(
                        List.of(
                            Literal.ofMap(
                                Map.of("A", stringLiteralOf("a"), "B", stringLiteralOf("b"))),
                            Literal.ofMap(
                                Map.of("a", stringLiteralOf("A"), "b", stringLiteralOf("B")))))),
                hasEntry(
                    "ml",
                    Literal.ofMap(
                        Map.of(
                            "frodo",
                            Literal.ofCollection(
                                List.of(stringLiteralOf("baggins"), stringLiteralOf("bolson")))))),
                hasEntry(
                    "mm",
                    Literal.ofMap(
                        Map.of(
                            "math",
                            Literal.ofMap(
                                Map.of(
                                    "pi", stringLiteralOf("3.14"), "e", stringLiteralOf("2.72"))),
                            "pokemon",
                            Literal.ofMap(Map.of("ash", stringLiteralOf("pikachu"))))))
                // hasEntry("blob", literalOf(blob))
                )));
  }

  @Test
  public void testToSdkBindingDataMap() {
    AutoValueInput input =
        createAutoValueInput(
            /* i= */ 42L,
            /* f= */ 42.0d,
            /* s= */ "42",
            /* b= */ false,
            /* t= */ Instant.ofEpochSecond(42, 1),
            /* d= */ Duration.ofSeconds(1, 42),
            /// * blob= */ blob,
            /* l= */ List.of("foo"),
            /* m= */ Map.of("marco", "polo"),
            /* ll= */ List.of(List.of("foo", "bar"), List.of("a", "b", "c")),
            /* lm= */ List.of(Map.of("A", "a", "B", "b"), Map.of("a", "A", "b", "B")),
            /* ml= */ Map.of("frodo", List.of("baggins", "bolson")),
            /* mm= */ Map.of(
                "math", Map.of("pi", "3.14", "e", "2.72"), "pokemon", Map.of("ash", "pikachu")));

    Map<String, SdkBindingData<?>> sdkBindingDataMap =
        JacksonSdkType.of(AutoValueInput.class).toSdkBindingMap(input);

    Map<String, SdkBindingData<?>> expected = new HashMap<>();
    expected.put("i", input.i());
    expected.put("f", input.f());
    expected.put("s", input.s());
    expected.put("b", input.b());
    expected.put("t", input.t());
    expected.put("d", input.d());
    expected.put("l", input.l());
    expected.put("m", input.m());
    expected.put("ll", input.ll());
    expected.put("lm", input.lm());
    expected.put("ml", input.ml());
    expected.put("mm", input.mm());

    assertEquals(expected, sdkBindingDataMap);
  }

  @Test
  public void testToSdkBindingDataMapJsonProperties() {
    JsonPropertyClassInput input =
        new JsonPropertyClassInput(
            SdkBindingDatas.ofString("test"), SdkBindingDatas.ofString("name"));

    Map<String, SdkBindingData<?>> sdkBindingDataMap =
        JacksonSdkType.of(JsonPropertyClassInput.class).toSdkBindingMap(input);

    var expected = Map.of("test", input.test, "name", input.otherTest);

    assertEquals(expected, sdkBindingDataMap);
  }

  public static class JsonPropertyClassInput {
    @JsonProperty final SdkBindingData<String> test;

    @JsonProperty("name")
    final SdkBindingData<String> otherTest;

    @JsonCreator
    public JsonPropertyClassInput(SdkBindingData<String> test, SdkBindingData<String> otherTest) {
      this.test = test;
      this.otherTest = otherTest;
    }
  }

  @Test
  public void testPojoToLiteralMap() {
    PojoInput input = new PojoInput();
    input.a = SdkBindingDatas.ofInteger(42);

    Map<String, Literal> literalMap = JacksonSdkType.of(PojoInput.class).toLiteralMap(input);

    assertThat(literalMap, equalTo(Map.of("a", literalOf(Primitive.ofIntegerValue(42)))));
  }

  @Test
  public void testPojoFromLiteralMap() {
    PojoInput expected = new PojoInput();
    expected.a = SdkBindingDatas.ofInteger(42);

    PojoInput pojoInput =
        JacksonSdkType.of(PojoInput.class)
            .fromLiteralMap(Map.of("a", literalOf(Primitive.ofIntegerValue(42))));

    assertThat(pojoInput.a.get(), equalTo(expected.a.get()));
  }

  @Test
  public void testPojoVariableMap() {
    Variable expected =
        Variable.builder().description("a description").literalType(LiteralTypes.INTEGER).build();

    Map<String, Variable> variableMap = JacksonSdkType.of(PojoInput.class).getVariableMap();

    assertThat(variableMap, equalTo(Map.of("a", expected)));
  }

  @Disabled("Not supported struct with the strongly types implementation.")
  public void testStructRoundtrip() {
    fail();
    //    StructInput input =
    //        StructInput.create(
    //            null
    //            // StructValueInput.create(
    //            //    /* stringValue= */ "nested-string",
    //            //    /* boolValue= */ false,
    //            //    /* listValue= */ Arrays.asList(1L, 2L, 3L),
    //            //    /* structValue= */ StructValueInput.create(
    //            //        /* stringValue= */ "nested-string",
    //            //        /* boolValue= */ false,
    //            //        /* listValue= */ Arrays.asList(1L, 2L, 3L),
    //            //        /* structValue= */ null,
    //            //        /* numberValue= */ 42.0),
    //            //    /* numberValue= */ 42.0)
    //            );
    //
    //    SdkType<StructInput> sdkType = JacksonSdkType.of(StructInput.class);
    //    Map<String, Literal> literalMap = sdkType.toLiteralMap(input);
    //    assertThat(sdkType.fromLiteralMap(literalMap), equalTo(input));
  }

  @Disabled("Not supported customType & customEnum with the strongly types implementation.")
  public void testConverterToLiteralMap() {
    InputWithCustomType input = InputWithCustomType.create(CustomType.ONE, CustomEnum.TWO);
    Map<String, Literal> expected = new HashMap<>();
    expected.put("customType", literalOf(Primitive.ofStringValue("ONE")));
    expected.put("customEnum", literalOf(Primitive.ofStringValue("TWO")));

    Map<String, Literal> literalMap =
        JacksonSdkType.of(InputWithCustomType.class).toLiteralMap(input);

    assertThat(literalMap, equalTo(expected));
  }

  @Disabled("Not supported customType & customEnum with the strongly types implementation.")
  public void testConverterFromLiteralMap() {
    InputWithCustomType expected = InputWithCustomType.create(CustomType.TWO, CustomEnum.ONE);
    Map<String, Literal> literalMap = new HashMap<>();
    literalMap.put("customType", literalOf(Primitive.ofStringValue("TWO")));
    literalMap.put("customEnum", literalOf(Primitive.ofStringValue("ONE")));

    InputWithCustomType output =
        JacksonSdkType.of(InputWithCustomType.class).fromLiteralMap(literalMap);

    assertThat(output, equalTo(expected));
  }

  @Disabled("Not supported customType & customEnum with the strongly types implementation.")
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
  void testPromiseFor() {
    AutoValueInput autoValueInput = JacksonSdkType.of(AutoValueInput.class).promiseFor("node-id");

    assertThat(
        autoValueInput.i(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.integers(), "node-id", "i")));
    assertThat(
        autoValueInput.f(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.floats(), "node-id", "f")));
    assertThat(
        autoValueInput.s(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.strings(), "node-id", "s")));
    assertThat(
        autoValueInput.b(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.booleans(), "node-id", "b")));
    assertThat(
        autoValueInput.t(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.datetimes(), "node-id", "t")));
    assertThat(
        autoValueInput.d(),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.durations(), "node-id", "d")));
    assertThat(
        autoValueInput.l(),
        equalTo(
            SdkBindingData.promise(
                SdkLiteralTypes.collections(SdkLiteralTypes.strings()), "node-id", "l")));
    assertThat(
        autoValueInput.m(),
        equalTo(
            SdkBindingData.promise(
                SdkLiteralTypes.maps(SdkLiteralTypes.strings()), "node-id", "m")));
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

  @Test
  void rejectDeprecatedAutoValueInput() {

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> JacksonSdkType.of(AutoValueDeprecatedInput.class));

    assertThat(
        e.getMessage(),
        equalTo(
            "Field 'i' from class 'org.flyte.flytekit.jackson.JacksonSdkTypeTest$AutoValueDeprecatedInput'"
                + " is declared as 'long' and it is not matching any of the supported types. Please make sure your variable declared type is wrapped in 'SdkBindingData<>'."));
  }

  public static class Unannotated {}

  @AutoValue
  public abstract static class AutoValueDeprecatedInput {
    public abstract long i();

    public static AutoValueDeprecatedInput create(long i) {
      return new AutoValue_JacksonSdkTypeTest_AutoValueDeprecatedInput(i);
    }
  }

  @AutoValue
  public abstract static class AutoValueInput {

    @Description("input i")
    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<String> s();

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> t();

    public abstract SdkBindingData<Duration> d();

    // TODO add blobs to sdkbinding data
    // public abstract SdkBindingData<Blob> blob();

    public abstract SdkBindingData<List<String>> l();

    public abstract SdkBindingData<Map<String, String>> m();

    public abstract SdkBindingData<List<List<String>>> ll();

    public abstract SdkBindingData<List<Map<String, String>>> lm();

    public abstract SdkBindingData<Map<String, List<String>>> ml();

    public abstract SdkBindingData<Map<String, Map<String, String>>> mm();

    public static AutoValueInput create(
        SdkBindingData<Long> i,
        SdkBindingData<Double> f,
        SdkBindingData<String> s,
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> t,
        SdkBindingData<Duration> d,
        // Blob blob,
        SdkBindingData<List<String>> l,
        SdkBindingData<Map<String, String>> m,
        SdkBindingData<List<List<String>>> ll,
        SdkBindingData<List<Map<String, String>>> lm,
        SdkBindingData<Map<String, List<String>>> ml,
        SdkBindingData<Map<String, Map<String, String>>> mm) {
      return new AutoValue_JacksonSdkTypeTest_AutoValueInput(
          i, f, s, b, t, d, l, m, ll, lm, ml, mm);
    }
  }

  @AutoValue
  public abstract static class StructInput {
    public abstract SdkBindingData<StructValueInput> structLevel1();

    public static StructInput create(SdkBindingData<StructValueInput> structValue) {
      return new AutoValue_JacksonSdkTypeTest_StructInput(structValue);
    }
  }

  @AutoValue
  public abstract static class StructValueInput {
    public abstract String stringValue();

    public abstract boolean boolValue();

    public abstract List<Long> listValue();

    @Nullable
    public abstract StructValueInput structLevel();

    public abstract double numberValue();

    public static StructValueInput create(
        String stringValue,
        boolean boolValue,
        List<Long> listValue,
        StructValueInput structValue,
        Double numberValue) {
      return new AutoValue_JacksonSdkTypeTest_StructValueInput(
          stringValue, boolValue, listValue, structValue, numberValue);
    }
  }

  public static final class PojoInput {
    @Description("a description")
    public SdkBindingData<Long> a;

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
    return createVar(ofSimpleType(simpleType), "");
  }

  private static Variable createVar(SimpleType simpleType, String description) {
    return createVar(ofSimpleType(simpleType), description);
  }

  private static Variable createVar(LiteralType literalType) {
    return createVar(literalType, "");
  }

  private static Variable createVar(LiteralType literalType, String description) {
    return Variable.builder().literalType(literalType).description(description).build();
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }
}
