/*
 * Copyright 2023 Flyte Authors.
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
import static org.flyte.flytekit.SdkLiteralTypes.booleans;
import static org.flyte.flytekit.SdkLiteralTypes.collections;
import static org.flyte.flytekit.SdkLiteralTypes.datetimes;
import static org.flyte.flytekit.SdkLiteralTypes.durations;
import static org.flyte.flytekit.SdkLiteralTypes.floats;
import static org.flyte.flytekit.SdkLiteralTypes.integers;
import static org.flyte.flytekit.SdkLiteralTypes.maps;
import static org.flyte.flytekit.SdkLiteralTypes.strings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SdkBindingDataFactoryTest {

  @Test
  public void testOfBindingCollection() {
    List<Long> collection = List.of(42L, 1337L);

    SdkBindingData<List<Long>> output = SdkBindingDataFactory.ofIntegerCollection(collection);

    assertThat(output.get(), equalTo(collection));
    assertThat(output.type(), equalTo(collections(integers())));
  }

  @Test
  public void testOfBindingCollection_empty() {
    SdkBindingData<List<Long>> output =
        SdkBindingDataFactory.ofBindingCollection(integers(), emptyList());

    assertThat(output.get(), equalTo(List.of()));
    assertThat(output.type(), equalTo(collections(integers())));
  }

  @Test
  public void testOfStringCollection() {
    List<String> expectedValue = List.of("1", "2");

    SdkBindingData<List<String>> output = SdkBindingDataFactory.ofStringCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(strings())));
  }

  @Test
  public void testOfFloatCollection() {
    List<Double> expectedValue = List.of(1.1, 1.2);

    SdkBindingData<List<Double>> output = SdkBindingDataFactory.ofFloatCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(floats())));
  }

  @Test
  public void testOfIntegerCollection() {
    List<Long> expectedValue = List.of(1L, 2L);

    SdkBindingData<List<Long>> output = SdkBindingDataFactory.ofIntegerCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(integers())));
  }

  @Test
  public void testOfBooleanCollection() {
    List<Boolean> expectedValue = List.of(true, false);

    SdkBindingData<List<Boolean>> output = SdkBindingDataFactory.ofBooleanCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(booleans())));
  }

  @Test
  public void testOfDurationCollection() {
    List<Duration> expectedValue = List.of(Duration.ofDays(1), Duration.ofDays(2));

    SdkBindingData<List<Duration>> output =
        SdkBindingDataFactory.ofDurationCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(durations())));
  }

  @Test
  public void testOfDatetimeCollection() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    List<Instant> expectedValue = List.of(first, second);

    SdkBindingData<List<Instant>> output =
        SdkBindingDataFactory.ofDatetimeCollection(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(collections(datetimes())));
  }

  @Test
  public void testOfBindingMap() {
    Map<String, SdkBindingData<Long>> input =
        Map.of(
            "a", SdkBindingDataFactory.of(42L),
            "b", SdkBindingDataFactory.of(1337L));

    SdkBindingData<Map<String, Long>> output =
        SdkBindingDataFactory.ofBindingMap(integers(), input);

    assertThat(output.get(), equalTo(Map.of("a", 42L, "b", 1337L)));
    assertThat(output.type(), equalTo(maps(integers())));
  }

  @Test
  public void testOfBindingMap_empty() {
    SdkBindingData<Map<String, Long>> output =
        SdkBindingDataFactory.ofBindingMap(integers(), emptyMap());

    assertThat(output.get(), equalTo(Map.of()));
    assertThat(output.type(), equalTo(maps(integers())));
  }

  @Test
  public void testOfStringMap() {
    Map<String, String> expectedValue =
        Map.of(
            "a", "1",
            "b", "2");

    SdkBindingData<Map<String, String>> output = SdkBindingDataFactory.ofStringMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(strings())));
  }

  @Test
  public void testOfFloatMap() {
    Map<String, Double> expectedValue =
        Map.of(
            "a", 1.1,
            "b", 1.2);

    SdkBindingData<Map<String, Double>> output = SdkBindingDataFactory.ofFloatMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(floats())));
  }

  @Test
  public void testOfIntegerMap() {
    Map<String, Long> expectedValue = Map.of("a", 1L, "b", 2L);

    SdkBindingData<Map<String, Long>> output = SdkBindingDataFactory.ofIntegerMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(integers())));
  }

  @Test
  public void testOfBooleanMap() {
    Map<String, Boolean> expectedValue =
        Map.of(
            "a", true,
            "b", false);

    SdkBindingData<Map<String, Boolean>> output = SdkBindingDataFactory.ofBooleanMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(booleans())));
  }

  @Test
  public void testOfDurationMap() {
    Map<String, Duration> expectedValue =
        Map.of(
            "a", Duration.ofDays(1),
            "b", Duration.ofDays(2));

    SdkBindingData<Map<String, Duration>> output =
        SdkBindingDataFactory.ofDurationMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(durations())));
  }

  @Test
  public void testOfDatetimeMap() {
    Instant first = LocalDate.of(2022, 1, 16).atStartOfDay().toInstant(UTC);
    Instant second = LocalDate.of(2022, 1, 17).atStartOfDay().toInstant(UTC);

    Map<String, Instant> expectedValue =
        Map.of(
            "a", first,
            "b", second);

    SdkBindingData<Map<String, Instant>> output =
        SdkBindingDataFactory.ofDatetimeMap(expectedValue);

    assertThat(output.get(), equalTo(expectedValue));
    assertThat(output.type(), equalTo(maps(datetimes())));
  }
}
