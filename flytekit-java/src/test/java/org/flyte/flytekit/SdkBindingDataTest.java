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
import org.junit.jupiter.api.Test;

public class SdkBindingDataTest {

  @Test
  public void testOfBindingCollection() {
    List<SdkBindingData<Long>> input =
        Arrays.asList(SdkBindingDatas.ofInteger(42L), SdkBindingDatas.ofInteger(1337L));

    List<BindingData> expected =
        Arrays.asList(
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(42L))),
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));

    SdkBindingData<List<Long>> output =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.INTEGER, input);

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
        SdkBindingDatas.ofBindingCollection(LiteralTypes.INTEGER, emptyList());
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfStringCollection() {
    List<SdkBindingData<String>> input =
        List.of(SdkBindingDatas.ofString("1"), SdkBindingDatas.ofString("2"));

    List<String> expectedValue = List.of("1", "2");

    SdkBindingData<List<String>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.STRING, input);

    SdkBindingData<List<String>> output = SdkBindingDatas.ofStringCollection(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfFloatCollection() {
    List<SdkBindingData<Double>> input =
        List.of(SdkBindingDatas.ofFloat(1.1), SdkBindingDatas.ofFloat(1.2));

    List<Double> expectedValue = List.of(1.1, 1.2);

    SdkBindingData<List<Double>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.FLOAT, input);

    SdkBindingData<List<Double>> output = SdkBindingDatas.ofFloatCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfIntegerCollection() {
    List<SdkBindingData<Long>> input =
        List.of(SdkBindingDatas.ofInteger(1L), SdkBindingDatas.ofInteger(2L));

    List<Long> expectedValue = List.of(1L, 2L);

    SdkBindingData<List<Long>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.INTEGER, input);

    SdkBindingData<List<Long>> output = SdkBindingDatas.ofIntegerCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBooleanCollection() {
    List<SdkBindingData<Boolean>> input =
        List.of(SdkBindingDatas.ofBoolean(true), SdkBindingDatas.ofBoolean(false));

    List<Boolean> expectedValue = List.of(true, false);

    SdkBindingData<List<Boolean>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.BOOLEAN, input);

    SdkBindingData<List<Boolean>> output = SdkBindingDatas.ofBooleanCollection(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDurationCollection() {
    List<SdkBindingData<Duration>> input =
        List.of(
            SdkBindingDatas.ofDuration(Duration.ofDays(1)),
            SdkBindingDatas.ofDuration(Duration.ofDays(2)));

    List<Duration> expectedValue = List.of(Duration.ofDays(1), Duration.ofDays(2));

    SdkBindingData<List<Duration>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.DURATION, input);

    SdkBindingData<List<Duration>> output = SdkBindingDatas.ofDurationCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDatetimeCollection() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    List<SdkBindingData<Instant>> input =
        List.of(SdkBindingDatas.ofDatetime(first), SdkBindingDatas.ofDatetime(second));

    List<Instant> expectedValue = List.of(first, second);

    SdkBindingData<List<Instant>> expected =
        SdkBindingDatas.ofBindingCollection(LiteralTypes.DATETIME, input);

    SdkBindingData<List<Instant>> output = SdkBindingDatas.ofDatetimeCollection(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBindingMap() {
    Map<String, SdkBindingData<Long>> input =
        Map.of(
            "a", SdkBindingDatas.ofInteger(42L),
            "b", SdkBindingDatas.ofInteger(1337L));

    Map<String, BindingData> expected =
        Map.of(
            "a",
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(42L))),
            "b",
            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));

    SdkBindingData<Map<String, Long>> output =
        SdkBindingDatas.ofBindingMap(LiteralTypes.INTEGER, input);

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
        SdkBindingDatas.ofBindingMap(LiteralTypes.INTEGER, emptyMap());
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfStringMap() {
    Map<String, SdkBindingData<String>> input =
        Map.of(
            "a", SdkBindingDatas.ofString("1"),
            "b", SdkBindingDatas.ofString("2"));

    Map<String, String> expectedValue =
        Map.of(
            "a", "1",
            "b", "2");

    SdkBindingData<Map<String, String>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.STRING, input);

    SdkBindingData<Map<String, String>> output = SdkBindingDatas.ofStringMap(expectedValue);
    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfFloatMap() {
    Map<String, SdkBindingData<Double>> input =
        Map.of(
            "a", SdkBindingDatas.ofFloat(1.1),
            "b", SdkBindingDatas.ofFloat(1.2));

    Map<String, Double> expectedValue =
        Map.of(
            "a", 1.1,
            "b", 1.2);

    SdkBindingData<Map<String, Double>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.FLOAT, input);

    SdkBindingData<Map<String, Double>> output = SdkBindingDatas.ofFloatMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfIntegerMap() {
    Map<String, SdkBindingData<Long>> input =
        Map.of(
            "a", SdkBindingDatas.ofInteger(1L),
            "b", SdkBindingDatas.ofInteger(2L));

    Map<String, Long> expectedValue =
        Map.of(
            "a", 1L,
            "b", 2L);

    SdkBindingData<Map<String, Long>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.INTEGER, input);

    SdkBindingData<Map<String, Long>> output = SdkBindingDatas.ofIntegerMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfBooleanMap() {
    Map<String, SdkBindingData<Boolean>> input =
        Map.of(
            "a", SdkBindingDatas.ofBoolean(true),
            "b", SdkBindingDatas.ofBoolean(false));

    Map<String, Boolean> expectedValue =
        Map.of(
            "a", true,
            "b", false);

    SdkBindingData<Map<String, Boolean>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.BOOLEAN, input);

    SdkBindingData<Map<String, Boolean>> output = SdkBindingDatas.ofBooleanMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDurationMap() {
    Map<String, SdkBindingData<Duration>> input =
        Map.of(
            "a", SdkBindingDatas.ofDuration(Duration.ofDays(1)),
            "b", SdkBindingDatas.ofDuration(Duration.ofDays(2)));

    Map<String, Duration> expectedValue =
        Map.of(
            "a", Duration.ofDays(1),
            "b", Duration.ofDays(2));

    SdkBindingData<Map<String, Duration>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.DURATION, input);

    SdkBindingData<Map<String, Duration>> output = SdkBindingDatas.ofDurationMap(expectedValue);

    assertThat(output, equalTo(expected));
  }

  @Test
  public void testOfDatetimeMap() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    Map<String, SdkBindingData<Instant>> input =
        Map.of(
            "a", SdkBindingDatas.ofDatetime(first),
            "b", SdkBindingDatas.ofDatetime(second));

    Map<String, Instant> expectedValue =
        Map.of(
            "a", first,
            "b", second);

    SdkBindingData<Map<String, Instant>> expected =
        SdkBindingDatas.ofBindingMap(LiteralTypes.DATETIME, input);

    SdkBindingData<Map<String, Instant>> output = SdkBindingDatas.ofDatetimeMap(expectedValue);

    assertThat(output, equalTo(expected));
  }
}
