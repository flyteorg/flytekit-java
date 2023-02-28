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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class SdkLiteralTypesTest {

  @ParameterizedTest
  @ArgumentsSource(TestOfProvider.class)
  void testOf(SdkLiteralType<?> expected, SdkLiteralType<?> actual) {
    assertEquals(expected, actual);
  }

  static class TestOfProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
          Arguments.of(integers(), SdkLiteralTypes.of(Long.class)),
          Arguments.of(floats(), SdkLiteralTypes.of(Double.class)),
          Arguments.of(strings(), SdkLiteralTypes.of(String.class)),
          Arguments.of(booleans(), SdkLiteralTypes.of(Boolean.class)),
          Arguments.of(datetimes(), SdkLiteralTypes.of(Instant.class)),
          Arguments.of(durations(), SdkLiteralTypes.of(Duration.class)),
          Arguments.of(collections(integers()), collections(Long.class)),
          Arguments.of(collections(floats()), collections(Double.class)),
          Arguments.of(collections(strings()), collections(String.class)),
          Arguments.of(collections(booleans()), collections(Boolean.class)),
          Arguments.of(collections(datetimes()), collections(Instant.class)),
          Arguments.of(collections(durations()), collections(Duration.class)),
          Arguments.of(maps(integers()), maps(Long.class)),
          Arguments.of(maps(floats()), maps(Double.class)),
          Arguments.of(maps(strings()), maps(String.class)),
          Arguments.of(maps(booleans()), maps(Boolean.class)),
          Arguments.of(maps(datetimes()), maps(Instant.class)),
          Arguments.of(maps(durations()), maps(Duration.class)));
    }
  }

  @Test
  public void testIntegers() {
    assertThat(roundtrip(integers(), 42L), equalTo(42L));
  }

  @Test
  public void testDoubles() {
    assertThat(roundtrip(floats(), 1337.0), equalTo(1337.0));
  }

  @Test
  public void testBoolean() {
    assertThat(roundtrip(booleans(), true), equalTo(true));
  }

  @Test
  public void testStrings() {
    assertThat(roundtrip(strings(), "forty-two"), equalTo("forty-two"));
  }

  @Test
  public void testDatetimes() {
    assertThat(
        roundtrip(datetimes(), Instant.ofEpochSecond(1337L)),
        equalTo(Instant.ofEpochSecond(1337L)));
  }

  @Test
  public void testDuration() {
    assertThat(
        roundtrip(durations(), Duration.ofSeconds(1337L)), equalTo(Duration.ofSeconds(1337L)));
  }

  @Test
  public void testCollectionOfIntegers() {
    assertThat(
        roundtrip(collections(integers()), List.of(42L, 1337L)),
        equalTo(Arrays.asList(42L, 1337L)));
  }

  @Test
  public void testMapOfIntegers() {
    Map<String, Long> map = Map.of("a", 42L, "b", 1337L);

    assertThat(roundtrip(maps(integers()), map).entrySet(), equalTo(map.entrySet()));
  }

  public static <T> T roundtrip(SdkLiteralType<T> literalType, T value) {
    return literalType.fromLiteral(literalType.toLiteral(value));
  }
}
