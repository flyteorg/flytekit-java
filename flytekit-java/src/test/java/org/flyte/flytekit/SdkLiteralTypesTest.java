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
package org.flyte.flytekit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SdkLiteralTypesTest {

  @Test
  public void testIntegers() {
    assertThat(roundtrip(SdkLiteralTypes.integers(), 42L), equalTo(42L));
  }

  @Test
  public void testDoubles() {
    assertThat(roundtrip(SdkLiteralTypes.floats(), 1337.0), equalTo(1337.0));
  }

  @Test
  public void testBoolean() {
    assertThat(roundtrip(SdkLiteralTypes.booleans(), true), equalTo(true));
  }

  @Test
  public void testStrings() {
    assertThat(roundtrip(SdkLiteralTypes.strings(), "forty-two"), equalTo("forty-two"));
  }

  @Test
  public void testDatetimes() {
    assertThat(
        roundtrip(SdkLiteralTypes.datetimes(), Instant.ofEpochSecond(1337L)),
        equalTo(Instant.ofEpochSecond(1337L)));
  }

  @Test
  public void testDuration() {
    assertThat(
        roundtrip(SdkLiteralTypes.durations(), Duration.ofSeconds(1337L)),
        equalTo(Duration.ofSeconds(1337L)));
  }

  @Test
  public void testCollectionOfIntegers() {
    assertThat(
        roundtrip(
            SdkLiteralTypes.collections(SdkLiteralTypes.integers()), Arrays.asList(42L, 1337L)),
        equalTo(Arrays.asList(42L, 1337L)));
  }

  @Test
  public void testMapOfIntegers() {
    Map<String, Long> map = new LinkedHashMap<>();
    map.put("a", 42L);
    map.put("b", 1337L);

    assertThat(
        roundtrip(SdkLiteralTypes.maps(SdkLiteralTypes.integers()), map).entrySet(),
        equalTo(map.entrySet()));
  }

  public static <T> T roundtrip(SdkLiteralType<T> literalType, T value) {
    return literalType.fromLiteral(literalType.toLiteral(value));
  }
}
