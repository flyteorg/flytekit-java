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
package org.flyte.flytekit.jackson;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.api.v1.LiteralType.ofSimpleType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;

public class JacksonSdkTypeTest {

  @Test
  public void testVariableMap() {
    Map<String, Variable> expected = new HashMap<>();
    expected.put("i", createVar(SimpleType.INTEGER));
    expected.put("f", createVar(SimpleType.FLOAT));
    expected.put("s", createVar(SimpleType.STRING));
    expected.put("b", createVar(SimpleType.BOOLEAN));
    expected.put("t", createVar(SimpleType.DATETIME));
    expected.put("d", createVar(SimpleType.DURATION));
    expected.put("l", createVar(LiteralType.ofCollectionType(ofSimpleType(SimpleType.STRING))));
    expected.put("m", createVar(LiteralType.ofMapValueType(ofSimpleType(SimpleType.STRING))));

    assertEquals(expected, JacksonSdkType.of(AutoValueInput.class).getVariableMap());
  }

  @Test
  void testFromLiteralMap() {
    Instant datetime = Instant.ofEpochSecond(12, 34);
    Duration duration = Duration.ofSeconds(56, 78);
    Map<String, Literal> literalMap = new HashMap<>();
    literalMap.put("i", literalOf(Primitive.ofInteger(123L)));
    literalMap.put("f", literalOf(Primitive.ofFloat(123.0)));
    literalMap.put("s", literalOf(Primitive.ofString("123")));
    literalMap.put("b", literalOf(Primitive.ofBoolean(true)));
    literalMap.put("t", literalOf(Primitive.ofDatetime(datetime)));
    literalMap.put("d", literalOf(Primitive.ofDuration(duration)));
    literalMap.put("l", Literal.ofCollection(singletonList(literalOf(Primitive.ofString("123")))));
    literalMap.put(
        "m", Literal.ofMap(singletonMap("marco", literalOf(Primitive.ofString("polo")))));

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
                /* l= */ singletonList("123"),
                /* m= */ singletonMap("marco", "polo"))));
  }

  @Test
  void testToLiteralMap() {
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
                    /* l= */ singletonList("foo"),
                    /* m= */ singletonMap("marco", "polo")));

    Map<String, Literal> expected = new HashMap<>();
    expected.put("i", literalOf(Primitive.ofInteger(42L)));
    expected.put("f", literalOf(Primitive.ofFloat(42.0d)));
    expected.put("s", literalOf(Primitive.ofString("42")));
    expected.put("b", literalOf(Primitive.ofBoolean(false)));
    expected.put("t", literalOf(Primitive.ofDatetime(Instant.ofEpochSecond(42, 1))));
    expected.put("d", literalOf(Primitive.ofDuration(Duration.ofSeconds(1, 42))));
    expected.put("l", Literal.ofCollection(singletonList(literalOf(Primitive.ofString("foo")))));
    expected.put("m", Literal.ofMap(singletonMap("marco", literalOf(Primitive.ofString("polo")))));

    assertThat(literalMap, equalTo(expected));
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
  @JsonSerialize(as = AutoValueInput.class)
  @JsonDeserialize
  public abstract static class AutoValueInput {
    public abstract long getI();

    public abstract double getF();

    public abstract String getS();

    public abstract boolean getB();

    public abstract Instant getT();

    public abstract Duration getD();

    public abstract List<String> getL();

    public abstract Map<String, String> getM();

    @JsonCreator
    public static AutoValueInput create(
        long i,
        double f,
        String s,
        boolean b,
        Instant t,
        Duration d,
        List<String> l,
        Map<String, String> m) {
      return new AutoValue_JacksonSdkTypeTest_AutoValueInput(i, f, s, b, t, d, l, m);
    }
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
}
