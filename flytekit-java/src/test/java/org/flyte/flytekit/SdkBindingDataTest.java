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

import static java.time.ZoneOffset.UTC;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
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

    SdkBindingData<List<Long>> output =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER)), input);

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
    List<BindingData> expectedValue = emptyList();
    SdkBindingData<List<Long>> expected =
        SdkBindingData.create(
            BindingData.ofCollection(expectedValue),
            LiteralType.ofCollectionType(LiteralTypes.INTEGER),
            emptyList());
    SdkBindingData<List<Long>> output =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
            emptyList());
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfStringCollection() {
    List<SdkBindingData<String>> input =
        List.of(SdkBindingData.ofString("1"), SdkBindingData.ofString("2"));

    List<String> expectedValue = List.of("1", "2");

    SdkBindingData<List<String>> expected =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralTypes.STRING), input);

    SdkBindingData<List<String>> output = SdkBindingData.ofStringCollection(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfFloatCollection() {
    List<SdkBindingData<Double>> input =
        List.of(SdkBindingData.ofFloat(1.1), SdkBindingData.ofFloat(1.2));

    List<Double> expectedValue = List.of(1.1, 1.2);

    SdkBindingData<List<Double>> expected =
        SdkBindingData.ofBindingCollection(LiteralType.ofCollectionType(LiteralTypes.FLOAT), input);

    SdkBindingData<List<Double>> output = SdkBindingData.ofFloatCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfIntegerCollection() {
    List<SdkBindingData<Long>> input =
        List.of(SdkBindingData.ofInteger(1L), SdkBindingData.ofInteger(2L));

    List<Long> expectedValue = List.of(1L, 2L);

    SdkBindingData<List<Long>> expected =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralTypes.INTEGER), input);

    SdkBindingData<List<Long>> output = SdkBindingData.ofIntegerCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBooleanCollection() {
    List<SdkBindingData<Boolean>> input =
        List.of(SdkBindingData.ofBoolean(true), SdkBindingData.ofBoolean(false));

    List<Boolean> expectedValue = List.of(true, false);

    SdkBindingData<List<Boolean>> expected =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralTypes.BOOLEAN), input);

    SdkBindingData<List<Boolean>> output = SdkBindingData.ofBooleanCollection(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDurationCollection() {
    List<SdkBindingData<Duration>> input =
        List.of(
            SdkBindingData.ofDuration(Duration.ofDays(1)),
            SdkBindingData.ofDuration(Duration.ofDays(2)));

    List<Duration> expectedValue = List.of(Duration.ofDays(1), Duration.ofDays(2));

    SdkBindingData<List<Duration>> expected =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralTypes.BOOLEAN), input);

    SdkBindingData<List<Duration>> output = SdkBindingData.ofDurationCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDatetimeCollection() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    List<SdkBindingData<Instant>> input =
        List.of(SdkBindingData.ofDatetime(first), SdkBindingData.ofDatetime(second));

    List<Instant> expectedValue = List.of(first, second);

    SdkBindingData<List<Instant>> expected =
        SdkBindingData.ofBindingCollection(
            LiteralType.ofCollectionType(LiteralTypes.DATETIME), input);

    SdkBindingData<List<Instant>> output = SdkBindingData.ofDatetimeCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBindingMap() {
    Map<String, SdkBindingData<Long>> input =
        Map.of(
            "a", SdkBindingData.ofInteger(42L),
            "b", SdkBindingData.ofInteger(1337L));

    Map<String, BindingData> expected =
        Map.of(
            "a",
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(42L))),
            "b",
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));

    SdkBindingData<Map<String, Long>> output =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.INTEGER), input);

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
    Map<String, BindingData> expectedValue = emptyMap();
    SdkBindingData<Map<String, Long>> expected =
        SdkBindingData.create(
            BindingData.ofMap(expectedValue),
            LiteralType.ofMapValueType(LiteralTypes.INTEGER),
            emptyMap());

    SdkBindingData<Map<String, Long>> output =
        SdkBindingData.ofBindingMap(
            LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.INTEGER)), emptyMap());
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfStringMap() {
    Map<String, SdkBindingData<String>> input =
        Map.of(
            "a", SdkBindingData.ofString("1"),
            "b", SdkBindingData.ofString("2"));

    Map<String, String> expectedValue =
        Map.of(
            "a", "1",
            "b", "2");

    SdkBindingData<Map<String, String>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.STRING), input);

    SdkBindingData<Map<String, String>> output = SdkBindingData.ofStringMap(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfFloatMap() {
    Map<String, SdkBindingData<Double>> input =
        Map.of(
            "a", SdkBindingData.ofFloat(1.1),
            "b", SdkBindingData.ofFloat(1.2));

    Map<String, Double> expectedValue =
        Map.of(
            "a", 1.1,
            "b", 1.2);

    SdkBindingData<Map<String, Double>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.FLOAT), input);

    SdkBindingData<Map<String, Double>> output = SdkBindingData.ofFloatMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfIntegerMap() {
    Map<String, SdkBindingData<Long>> input =
        Map.of(
            "a", SdkBindingData.ofInteger(1L),
            "b", SdkBindingData.ofInteger(2L));

    Map<String, Long> expectedValue =
        Map.of(
            "a", 1L,
            "b", 2L);

    SdkBindingData<Map<String, Long>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.INTEGER), input);

    SdkBindingData<Map<String, Long>> output = SdkBindingData.ofIntegerMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBooleanMap() {
    Map<String, SdkBindingData<Boolean>> input =
        Map.of(
            "a", SdkBindingData.ofBoolean(true),
            "b", SdkBindingData.ofBoolean(false));

    Map<String, Boolean> expectedValue =
        Map.of(
            "a", true,
            "b", false);

    SdkBindingData<Map<String, Boolean>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.BOOLEAN), input);

    SdkBindingData<Map<String, Boolean>> output = SdkBindingData.ofBooleanMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDurationMap() {
    Map<String, SdkBindingData<Duration>> input =
        Map.of(
            "a", SdkBindingData.ofDuration(Duration.ofDays(1)),
            "b", SdkBindingData.ofDuration(Duration.ofDays(2)));

    Map<String, Duration> expectedValue =
        Map.of(
            "a", Duration.ofDays(1),
            "b", Duration.ofDays(2));

    SdkBindingData<Map<String, Duration>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.DURATION), input);

    SdkBindingData<Map<String, Duration>> output = SdkBindingData.ofDurationMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDatetimeMap() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    Map<String, SdkBindingData<Instant>> input =
        Map.of(
            "a", SdkBindingData.ofDatetime(first),
            "b", SdkBindingData.ofDatetime(second));

    Map<String, Instant> expectedValue =
        Map.of(
            "a", first,
            "b", second);

    SdkBindingData<Map<String, Instant>> expected =
        SdkBindingData.ofBindingMap(LiteralType.ofMapValueType(LiteralTypes.DATETIME), input);

    SdkBindingData<Map<String, Instant>> output = SdkBindingData.ofDatetimeMap(expectedValue);

    assertThat(output, equalTo(expected));
  }
}
