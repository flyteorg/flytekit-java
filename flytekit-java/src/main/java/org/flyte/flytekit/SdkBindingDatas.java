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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.LiteralType;

public final class SdkBindingDatas {

  private SdkBindingDatas() {
    // prevent instantiation
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte integer ({@link Long} for java) with the given
   * value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Long> ofInteger(long value) {
    return SdkBindingData.literal(SdkLiteralTypes.integers(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte float ({@link Double} for java) with the given
   * value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Double> ofFloat(double value) {
    return SdkBindingData.literal(SdkLiteralTypes.floats(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte String with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<String> ofString(String value) {
    return SdkBindingData.literal(SdkLiteralTypes.strings(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte boolean with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Boolean> ofBoolean(boolean value) {
    return SdkBindingData.literal(SdkLiteralTypes.booleans(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte Datetime ({@link Instant} for java) with the given
   * date at 00:00 on UTC.
   *
   * @param year the year to represent, from {@code Year.MIN_VALUE} to {@code Year.MAX_VALUE}
   * @param month the month-of-year to represent, from 1 (January) to 12 (December)
   * @param day the day-of-month to represent, from 1 to 31
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Instant> ofDatetime(int year, int month, int day) {
    Instant instant = LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    return ofDatetime(instant);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte Datetime ({@link Instant} for java) with the given
   * value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Instant> ofDatetime(Instant value) {
    return SdkBindingData.literal(SdkLiteralTypes.datetimes(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte Duration for java with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Duration> ofDuration(Duration value) {
    return SdkBindingData.literal(SdkLiteralTypes.durations(), value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection given a java {@code List<T>} and the
   * elements type.
   *
   * @param elementType a {@link SdkLiteralType} for the collection elements type.
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<List<T>> ofCollection(
      SdkLiteralType<T> elementType, List<T> collection) {
    return SdkBindingData.literal(SdkLiteralTypes.collections(elementType), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of string given a java {@code
   * List<String>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<String>> ofStringCollection(List<String> collection) {
    return ofCollection(SdkLiteralTypes.strings(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of float given a java {@code
   * List<Double>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Double>> ofFloatCollection(List<Double> collection) {
    return ofCollection(SdkLiteralTypes.floats(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of integer given a java {@code
   * List<Long>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Long>> ofIntegerCollection(List<Long> collection) {
    return ofCollection(SdkLiteralTypes.integers(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of boolean given a java {@code
   * List<Boolean>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Boolean>> ofBooleanCollection(List<Boolean> collection) {
    return ofCollection(SdkLiteralTypes.booleans(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of Duration given a java {@code
   * List<Duration>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Duration>> ofDurationCollection(List<Duration> collection) {
    return ofCollection(SdkLiteralTypes.durations(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of datetime given a java {@code
   * List<Instant>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Instant>> ofDatetimeCollection(List<Instant> collection) {
    return ofCollection(SdkLiteralTypes.datetimes(), collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map given a java {@code Map<String, T>} and a
   * function to know how to convert each entry values form such map to a {@code SdkBindingData}.
   *
   * @param valuesType literal type for the values of the map, keys are always strings.
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<Map<String, T>> ofMap(
      SdkLiteralType<T> valuesType, Map<String, T> map) {
    return SdkBindingData.literal(SdkLiteralTypes.maps(valuesType), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of string given a java {@code Map<String,
   * String>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, String>> ofStringMap(Map<String, String> map) {
    return ofMap(SdkLiteralTypes.strings(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of float given a java {@code Map<String,
   * Double>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Double>> ofFloatMap(Map<String, Double> map) {
    return ofMap(SdkLiteralTypes.floats(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of integer given a java {@code Map<String,
   * Long>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Long>> ofIntegerMap(Map<String, Long> map) {
    return ofMap(SdkLiteralTypes.integers(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of boolean given a java {@code Map<String,
   * Boolean>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Boolean>> ofBooleanMap(Map<String, Boolean> map) {
    return ofMap(SdkLiteralTypes.booleans(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of duration given a java {@code Map<String,
   * Duration>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Duration>> ofDurationMap(Map<String, Duration> map) {
    return ofMap(SdkLiteralTypes.durations(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of datetime given a java {@code Map<String,
   * Instant>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Instant>> ofDatetimeMap(Map<String, Instant> map) {
    return ofMap(SdkLiteralTypes.datetimes(), map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection given a java {@code
   * List<SdkBindingData<T>>} and a literalType tp be used.
   *
   * @param elements collection to represent on this data.
   * @param elementType literal type for the elements in the collection.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<List<T>> ofBindingCollection(
      SdkLiteralType<T> elementType, List<SdkBindingData<T>> elements) {
    return SdkBindingData.bindingCollection(elementType, elements);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map given a java {@code Map<String,
   * SdkBindingData<T>>} and a literalType tp be used.
   *
   * @param valueMap collection to represent on this data.
   * @param valuesType literal type for the whole map. It must be a {@link
   *     LiteralType.Kind#MAP_VALUE_TYPE}.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<Map<String, T>> ofBindingMap(
      SdkLiteralType<T> valuesType, Map<String, SdkBindingData<T>> valueMap) {

    return SdkBindingData.bindingMap(valuesType, valueMap);
  }
}
