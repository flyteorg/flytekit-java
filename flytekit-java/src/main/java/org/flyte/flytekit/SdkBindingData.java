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

import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

@AutoValue
public abstract class SdkBindingData {

  abstract BindingData idl();

  abstract LiteralType type();

  public static SdkBindingData create(BindingData idl, LiteralType type) {
    return new AutoValue_SdkBindingData(idl, type);
  }

  public static SdkBindingData ofInteger(long value) {
    return ofPrimitive(Primitive.ofInteger(value));
  }

  public static SdkBindingData ofFloat(double value) {
    return ofPrimitive(Primitive.ofFloat(value));
  }

  public static SdkBindingData ofString(String value) {
    return ofPrimitive(Primitive.ofString(value));
  }

  public static SdkBindingData ofBoolean(boolean value) {
    return ofPrimitive(Primitive.ofBoolean(value));
  }

  public static SdkBindingData ofDatetime(int year, int month, int day) {
    Instant instant = LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    return ofDatetime(instant);
  }

  public static SdkBindingData ofDatetime(Instant value) {
    return ofPrimitive(Primitive.ofDatetime(value));
  }

  public static SdkBindingData ofDuration(Duration value) {
    return ofPrimitive(Primitive.ofDuration(value));
  }

  public static SdkBindingData ofOutputReference(String nodeId, String nodeVar, LiteralType type) {
    BindingData idl =
        BindingData.ofOutputReference(
            OutputReference.builder().nodeId(nodeId).var(nodeVar).build());
    return create(idl, type);
  }

  public static SdkBindingData ofPrimitive(Primitive primitive) {
    BindingData bindingData = BindingData.ofScalar(Scalar.ofPrimitive(primitive));
    LiteralType literalType = LiteralType.ofSimpleType(primitive.type());
    return create(bindingData, literalType);
  }

  public static SdkBindingData ofIntegerCollection(List<Long> elements) {
    return ofCollection(elements, Primitive::ofInteger, LiteralTypes.INTEGER);
  }

  public static SdkBindingData ofDoubleCollection(List<Double> elements) {
    return ofCollection(elements, Primitive::ofFloat, LiteralTypes.FLOAT);
  }

  public static SdkBindingData ofStringCollection(List<String> elements) {
    return ofCollection(elements, Primitive::ofString, LiteralTypes.STRING);
  }

  public static SdkBindingData ofBooleanCollection(List<Boolean> elements) {
    return ofCollection(elements, Primitive::ofBoolean, LiteralTypes.BOOLEAN);
  }

  public static SdkBindingData ofDatetimeCollection(List<Instant> elements) {
    return ofCollection(elements, Primitive::ofDatetime, LiteralTypes.DATETIME);
  }

  public static SdkBindingData ofDurationCollection(List<Duration> elements) {
    return ofCollection(elements, Primitive::ofDuration, LiteralTypes.DURATION);
  }

  private static <T> SdkBindingData ofCollection(
      List<T> elements, Function<T, Primitive> f, LiteralType type) {
    BindingData bindingData =
        BindingData.ofCollection(
            elements.stream()
                .map(elem -> BindingData.ofScalar(Scalar.ofPrimitive(f.apply(elem))))
                .collect(Collectors.toList()));
    LiteralType literalType = LiteralType.ofCollectionType(type);
    return create(bindingData, literalType);
  }

  public static SdkBindingData ofIntegerMap(Map<String, Long> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofInteger, LiteralTypes.INTEGER);
  }

  public static SdkBindingData ofDoubleMap(Map<String, Double> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofFloat, LiteralTypes.FLOAT);
  }

  public static SdkBindingData ofStringMap(Map<String, String> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofString, LiteralTypes.STRING);
  }

  public static SdkBindingData ofBooleanMap(Map<String, Boolean> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofBoolean, LiteralTypes.BOOLEAN);
  }

  public static SdkBindingData ofDatetimeMap(Map<String, Instant> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofDatetime, LiteralTypes.DATETIME);
  }

  public static SdkBindingData ofDurationMap(Map<String, Duration> elementsByKey) {
    return ofMap(elementsByKey, Primitive::ofDuration, LiteralTypes.DURATION);
  }

  private static <T> SdkBindingData ofMap(
      Map<String, T> elementsByKey, Function<T, Primitive> f, LiteralType type) {
    BindingData bindingData =
        BindingData.ofMap(
            elementsByKey.entrySet().stream()
                .map(
                    entry ->
                        new SimpleImmutableEntry<>(
                            entry.getKey(),
                            BindingData.ofScalar(Scalar.ofPrimitive(f.apply(entry.getValue())))))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    LiteralType literalType = LiteralType.ofMapValueType(type);
    return create(bindingData, literalType);
  }
}
