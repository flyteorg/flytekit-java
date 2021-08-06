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

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;

@AutoValue
public abstract class SdkBindingData {

  abstract BindingData idl();

  abstract LiteralType type();

  public static SdkBindingData create(BindingData idl, LiteralType type) {
    return new AutoValue_SdkBindingData(idl, type);
  }

  public static SdkBindingData ofInteger(long value) {
    return ofPrimitive(Primitive.ofIntegerValue(value));
  }

  public static SdkBindingData ofFloat(double value) {
    return ofPrimitive(Primitive.ofFloatValue(value));
  }

  public static SdkBindingData ofString(String value) {
    return ofPrimitive(Primitive.ofStringValue(value));
  }

  public static SdkBindingData ofBoolean(boolean value) {
    return ofPrimitive(Primitive.ofBooleanValue(value));
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
    LiteralType literalType = LiteralType.ofSimpleType(getSimpleType(primitive.kind()));

    return create(bindingData, literalType);
  }

  public static SdkBindingData ofStruct(SdkStruct struct) {
    BindingData bindingData = BindingData.ofScalar(Scalar.ofGeneric(struct.struct()));
    LiteralType literalType = LiteralType.ofSimpleType(SimpleType.STRUCT);

    return create(bindingData, literalType);
  }

  private static SimpleType getSimpleType(Primitive.Kind kind) {
    switch (kind) {
      case INTEGER_VALUE:
        return SimpleType.INTEGER;
      case FLOAT_VALUE:
        return SimpleType.FLOAT;
      case STRING_VALUE:
        return SimpleType.STRING;
      case BOOLEAN_VALUE:
        return SimpleType.BOOLEAN;
      case DATETIME:
        return SimpleType.DATETIME;
      case DURATION:
        return SimpleType.DURATION;
    }

    throw new AssertionError("Unexpected Primitive.Kind: " + kind);
  }

  public static SdkBindingData ofBindingCollection(List<SdkBindingData> elements) {
    // TODO we can fix that by introducing "top type" into type system and
    // implementing type casting in SDK, for now, we fail

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't create binding for an empty list without knowing the type, "
              + "to create an empty map use `of<type>Collection` instead");
    }

    return ofBindingCollection(elements, elements.iterator().next().type());
  }

  public static SdkBindingData ofIntegerCollection(List<Long> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofInteger, LiteralTypes.INTEGER);
  }

  public static SdkBindingData ofDoubleCollection(List<Double> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofFloat, LiteralTypes.FLOAT);
  }

  public static SdkBindingData ofStringCollection(List<String> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofString, LiteralTypes.STRING);
  }

  public static SdkBindingData ofBooleanCollection(List<Boolean> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofBoolean, LiteralTypes.BOOLEAN);
  }

  public static SdkBindingData ofDatetimeCollection(List<Instant> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofDatetime, LiteralTypes.DATETIME);
  }

  public static SdkBindingData ofDurationCollection(List<Duration> elements) {
    return ofBindingCollection(elements, SdkBindingData::ofDuration, LiteralTypes.DURATION);
  }

  private static <T> SdkBindingData ofBindingCollection(
      List<T> elements, Function<T, SdkBindingData> f, LiteralType literalType) {
    List<SdkBindingData> bindings = elements.stream().map(f).collect(toUnmodifiableList());

    return ofBindingCollection(bindings, literalType);
  }

  private static SdkBindingData ofBindingCollection(
      List<SdkBindingData> elements, LiteralType elementType) {
    List<BindingData> elementsIdl = new ArrayList<>();

    for (SdkBindingData element : elements) {
      if (!element.type().equals(elementType)) {
        String message =
            String.format("Type %s doesn't match expected type %s", element.type(), elementType);

        throw new IllegalArgumentException(message);
      }

      elementsIdl.add(element.idl());
    }

    return create(
        BindingData.ofCollection(unmodifiableList(elementsIdl)),
        LiteralType.ofCollectionType(elementType));
  }

  public static SdkBindingData ofBindingMap(Map<String, SdkBindingData> map) {
    // TODO we can fix that by introducing "top type" into type system and
    // implementing type casting in SDK, for now, we fail

    if (map.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't create binding for an empty map without knowing the type, "
              + "to create an empty map use `of<type>Map` instead");
    }

    return ofBindingMap(map, map.values().iterator().next().type());
  }

  public static SdkBindingData ofIntegerMap(Map<String, Long> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofInteger, LiteralTypes.INTEGER);
  }

  public static SdkBindingData ofDoubleMap(Map<String, Double> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofFloat, LiteralTypes.FLOAT);
  }

  public static SdkBindingData ofStringMap(Map<String, String> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofString, LiteralTypes.STRING);
  }

  public static SdkBindingData ofBooleanMap(Map<String, Boolean> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofBoolean, LiteralTypes.BOOLEAN);
  }

  public static SdkBindingData ofDatetimeMap(Map<String, Instant> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofDatetime, LiteralTypes.DATETIME);
  }

  public static SdkBindingData ofDurationMap(Map<String, Duration> elementsByKey) {
    return ofBindingMap(elementsByKey, SdkBindingData::ofDuration, LiteralTypes.DURATION);
  }

  private static <T> SdkBindingData ofBindingMap(
      Map<String, T> elementsByKey, Function<T, SdkBindingData> f, LiteralType mapValueType) {
    Map<String, SdkBindingData> map = new LinkedHashMap<>();

    for (Map.Entry<String, T> entry : elementsByKey.entrySet()) {
      map.put(entry.getKey(), f.apply(entry.getValue()));
    }

    return ofBindingMap(map, mapValueType);
  }

  private static SdkBindingData ofBindingMap(
      Map<String, SdkBindingData> map, LiteralType mapValueType) {
    Map<String, BindingData> mapIdl = new LinkedHashMap<>();

    for (Map.Entry<String, SdkBindingData> entry : map.entrySet()) {
      if (!entry.getValue().type().equals(mapValueType)) {
        String message =
            String.format(
                "Key [%s] (type %s) doesn't match expected type %s",
                entry.getKey(), entry.getValue().type(), mapValueType);

        throw new IllegalArgumentException(message);
      }

      mapIdl.put(entry.getKey(), entry.getValue().idl());
    }

    return create(
        BindingData.ofMap(unmodifiableMap(mapIdl)), LiteralType.ofMapValueType(mapValueType));
  }
}
