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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;

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
    return ofPrimitive(Primitive::ofIntegerValue, value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte float ({@link Double} for java) with the given
   * value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Double> ofFloat(double value) {
    return ofPrimitive(Primitive::ofFloatValue, value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte String with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<String> ofString(String value) {
    return ofPrimitive(Primitive::ofStringValue, value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte boolean with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Boolean> ofBoolean(boolean value) {
    return ofPrimitive(Primitive::ofBooleanValue, value);
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
    return ofPrimitive(Primitive::ofDatetime, value);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte Duration for java with the given value.
   *
   * @param value the simple value for this data
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Duration> ofDuration(Duration value) {
    return ofPrimitive(Primitive::ofDuration, value);
  }

  private static <T> SdkBindingData<T> ofPrimitive(Function<T, Primitive> toPrimitive, T value) {
    Primitive primitive = toPrimitive.apply(value);
    BindingData bindingData = BindingData.ofScalar(Scalar.ofPrimitive(primitive));
    LiteralType literalType = LiteralType.ofSimpleType(getSimpleType(primitive.kind()));

    return SdkBindingData.create(bindingData, literalType, value);
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
    return ofBindingCollection(
        elementType.getLiteralType(),
        collection.stream().map(elementType::toSdkBinding).collect(toUnmodifiableList()));
  }

  private static <T> SdkBindingData<List<T>> createCollection(
      List<T> collection, LiteralType literalType, Function<T, BindingData> bindingDataFn) {
    return SdkBindingData.create(
        BindingData.ofCollection(
            collection.stream().map(bindingDataFn).collect(toUnmodifiableList())),
        literalType,
        collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of string given a java {@code
   * List<String>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<String>> ofStringCollection(List<String> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.STRING)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of float given a java {@code
   * List<Double>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Double>> ofFloatCollection(List<Double> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.FLOAT)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of integer given a java {@code
   * List<Long>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Long>> ofIntegerCollection(List<Long> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of boolean given a java {@code
   * List<Boolean>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Boolean>> ofBooleanCollection(List<Boolean> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.BOOLEAN)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of Duration given a java {@code
   * List<Duration>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Duration>> ofDurationCollection(List<Duration> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.DURATION)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte collection of datetime given a java {@code
   * List<Instant>}.
   *
   * @param collection collection to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<List<Instant>> ofDatetimeCollection(List<Instant> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.DATETIME)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(value))));
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
    return ofBindingMap(
        valuesType.getLiteralType(),
        map.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), valuesType.toSdkBinding(e.getValue())))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  private static <T> SdkBindingData<Map<String, T>> createMap(
      Map<String, T> map, LiteralType literalType, Function<T, BindingData> bindingDataFn) {
    return SdkBindingData.create(
        BindingData.ofMap(
            map.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), bindingDataFn.apply(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
        literalType,
        map);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of string given a java {@code Map<String,
   * String>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, String>> ofStringMap(Map<String, String> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.STRING)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of float given a java {@code Map<String,
   * Double>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Double>> ofFloatMap(Map<String, Double> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.FLOAT)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of integer given a java {@code Map<String,
   * Long>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Long>> ofIntegerMap(Map<String, Long> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of boolean given a java {@code Map<String,
   * Boolean>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Boolean>> ofBooleanMap(Map<String, Boolean> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.BOOLEAN)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of duration given a java {@code Map<String,
   * Duration>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Duration>> ofDurationMap(Map<String, Duration> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.DURATION)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(value))));
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map of datetime given a java {@code Map<String,
   * Instant>}.
   *
   * @param map map to represent on this data.
   * @return the new {@code SdkBindingData}
   */
  public static SdkBindingData<Map<String, Instant>> ofDatetimeMap(Map<String, Instant> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.DATETIME)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(value))));
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
      LiteralType elementType, List<SdkBindingData<T>> elements) {
    // TODO: I would like to make private this method
    List<BindingData> bindings = elements.stream().map(SdkBindingData::idl).collect(toList());
    BindingData bindingData = BindingData.ofCollection(bindings);

    checkIncompatibleTypes(elementType, elements);
    boolean hasPromise = bindings.stream().anyMatch(SdkBindingDatas::isAPromise);
    List<T> unwrappedElements =
        hasPromise ? null : elements.stream().map(SdkBindingData::get).collect(toList());

    return SdkBindingData.create(
        bindingData, LiteralType.ofCollectionType(elementType), unwrappedElements);
  }

  private static <T> void checkIncompatibleTypes(
      LiteralType literalType, Collection<SdkBindingData<T>> elements) {
    List<LiteralType> incompatibleTypes =
        elements.stream()
            .map(SdkBindingData::type)
            .distinct()
            .filter(type -> !type.equals(literalType))
            .collect(toList());
    if (!incompatibleTypes.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Type mismatch: expected all elements of type %s but found some elements of type: %s",
              literalType, incompatibleTypes));
    }
  }

  private static boolean isAPromise(BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return false;
      case PROMISE:
        return true;
      case COLLECTION:
        return bindingData.collection().stream().anyMatch(SdkBindingDatas::isAPromise);
      case MAP:
        return bindingData.map().values().stream().anyMatch(SdkBindingDatas::isAPromise);
    }
    throw new IllegalArgumentException("BindingData.Kind not recognized: " + bindingData.kind());
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
      LiteralType valuesType, Map<String, SdkBindingData<T>> valueMap) {

    Map<String, BindingData> bindings =
        valueMap.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().idl()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    BindingData bindingData = BindingData.ofMap(bindings);

    checkIncompatibleTypes(valuesType, valueMap.values());
    boolean hasPromise = bindings.values().stream().anyMatch(SdkBindingDatas::isAPromise);
    Map<String, T> unwrappedElements =
        hasPromise
            ? null
            : valueMap.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().get()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    return SdkBindingData.create(
        bindingData, LiteralType.ofMapValueType(valuesType), unwrappedElements);
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte output reference.
   *
   * @param nodeId references to what node id this reference points to.
   * @param nodeVar name of the output variable that this reference points to.
   * @param type literal type of the referenced variable.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<T> ofOutputReference(
      String nodeId, String nodeVar, LiteralType type) {
    BindingData idl =
        BindingData.ofOutputReference(
            OutputReference.builder().nodeId(nodeId).var(nodeVar).build());
    // promises don't contain values yet
    return SdkBindingData.create(idl, type, null);
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
}
