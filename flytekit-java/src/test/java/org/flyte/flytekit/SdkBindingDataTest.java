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
package org.flyte.flytekit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.junit.jupiter.api.Test;

public class SdkBindingDataTest {

  @Test
  public void testOfBindingCollection() {
    List<SdkBindingData<Long>> input =
        Arrays.asList(SdkBindingData.ofInteger(42L), SdkBindingData.ofInteger(1337L));

    List<BindingData> expected =
        Arrays.asList(
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(42L))),
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));

    SdkBindingData<List<Long>> output = SdkBindingData.ofBindingCollection(input);

    assertThat(
        output,
        equalTo(
            SdkBindingData.create(
                BindingData.ofCollection(expected),
                LiteralType.ofCollectionType(LiteralTypes.INTEGER),
                List.of(42L, 1337L))));
  }

  @Test
  public void testOfBindingCollection_empty() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SdkBindingData.ofBindingCollection(emptyList()));

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't create binding for an empty list without knowing the type, "
                + "to create an empty map use `of<type>Collection` instead"));
  }

  @Test
  public void testOfBindingMap() {
    Map<String, SdkBindingData<Long>> input = new HashMap<>();
    input.put("a", SdkBindingData.ofInteger(42L));
    input.put("b", SdkBindingData.ofInteger(1337L));

    Map<String, BindingData> expected = new HashMap<>();
    expected.put("a", BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(42L))));
    expected.put("b", BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));

    SdkBindingData<Map<String, Long>> output = SdkBindingData.ofBindingMap(input);

    assertThat(
        output,
        equalTo(
            SdkBindingData.create(
                BindingData.ofMap(expected),
                LiteralType.ofMapValueType(LiteralTypes.INTEGER),
                Map.of("a", 42L, "b", 1337L))));
  }

  @Test
  public void testOfBindingMap_empty() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SdkBindingData.ofBindingMap(emptyMap()));

    assertThat(
        e.getMessage(),
        equalTo(
            "Can't create binding for an empty map without knowing the type, "
                + "to create an empty map use `of<type>Map` instead"));
  }

  @Test
  public void testOfStruct() {
    Map<String, Struct.Value> structFields = new LinkedHashMap<>();
    Map<String, Struct.Value> structFieldsNested = new LinkedHashMap<>();
    structFields.put("a", Struct.Value.ofBoolValue(true));
    structFields.put("b", Struct.Value.ofStructValue(Struct.of(structFieldsNested)));
    structFieldsNested.put("b_nested", Struct.Value.ofNumberValue(42));

    Struct expected = Struct.of(structFields);

    SdkStruct input =
        SdkStruct.builder()
            .addBooleanField("a", true)
            .addStructField("b", SdkStruct.builder().addIntegerField("b_nested", 42L).build())
            .build();

    SdkBindingData<SdkStruct> output = SdkBindingData.ofStruct(input);

    assertThat(
        output,
        equalTo(
            SdkBindingData.create(
                BindingData.ofScalar(Scalar.ofGeneric(expected)),
                LiteralType.ofSimpleType(SimpleType.STRUCT),
                input)));
  }
}
