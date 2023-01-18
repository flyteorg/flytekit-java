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

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;

@AutoValue
public abstract class SdkBindingData<T> {

  abstract BindingData idl();

  abstract LiteralType type();

  @Nullable
  abstract T value();

  public static <T> SdkBindingData<T> create(BindingData idl, LiteralType type, T value) {
    return new AutoValue_SdkBindingData<>(idl, type, value);
  }

  public static SdkBindingData<Long> ofInteger(long value) {
    return ofPrimitive(Primitive::ofIntegerValue, value);
  }

  public static SdkBindingData<Double> ofFloat(double value) {
    return ofPrimitive(Primitive::ofFloatValue, value);
  }

  public static SdkBindingData<String> ofString(String value) {
    return ofPrimitive(Primitive::ofStringValue, value);
  }

  public static SdkBindingData<Boolean> ofBoolean(boolean value) {
    return ofPrimitive(Primitive::ofBooleanValue, value);
  }

  public static SdkBindingData<Instant> ofDatetime(int year, int month, int day) {
    Instant instant = LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    return ofDatetime(instant);
  }

  public static SdkBindingData<Instant> ofDatetime(Instant value) {
    return ofPrimitive(Primitive::ofDatetime, value);
  }

  public static SdkBindingData<Duration> ofDuration(Duration value) {
    return ofPrimitive(Primitive::ofDuration, value);
  }

  private static <T> SdkBindingData<T> ofPrimitive(Function<T, Primitive> toPrimitive, T value) {
    Primitive primitive = toPrimitive.apply(value);
    BindingData bindingData = BindingData.ofScalar(Scalar.ofPrimitive(primitive));
    LiteralType literalType = LiteralType.ofSimpleType(getSimpleType(primitive.kind()));

    return create(bindingData, literalType, value);
  }

  public static <T> SdkBindingData<List<T>> ofCollection(
      List<T> collection, LiteralType literalType, Function<T, SdkBindingData<T>> function) {
    return SdkBindingData.ofBindingCollection(
        literalType, collection.stream().map(function).collect(Collectors.toList()));
  }

  private static <T> SdkBindingData<List<T>> createCollection(
      List<T> collection, LiteralType literalType, Function<T, BindingData> bindingDataFn) {
    return create(
        BindingData.ofCollection(
            collection.stream().map(bindingDataFn).collect(Collectors.toList())),
        literalType,
        collection);
  }

  public static SdkBindingData<List<String>> ofStringCollection(List<String> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.STRING)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(value))));
  }

  public static SdkBindingData<List<Double>> ofFloatCollection(List<Double> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.FLOAT)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))));
  }

  public static SdkBindingData<List<Long>> ofIntegerCollection(List<Long> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))));
  }

  public static SdkBindingData<List<Boolean>> ofBooleanCollection(List<Boolean> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.BOOLEAN)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))));
  }

  public static SdkBindingData<List<Duration>> ofDurationCollection(List<Duration> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.BOOLEAN)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(value))));
  }

  public static SdkBindingData<List<Instant>> ofDatetimeCollection(List<Instant> collection) {
    return createCollection(
        collection,
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.DATETIME)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(value))));
  }

  public static <T> SdkBindingData<Map<String, T>> ofMap(
      Map<String, T> map, LiteralType literalType, Function<T, SdkBindingData<T>> bindingFunction) {
    return SdkBindingData.ofBindingMap(
        literalType,
        map.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), bindingFunction.apply(e.getValue())))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  private static <T> SdkBindingData<Map<String, T>> createMap(
      Map<String, T> map, LiteralType literalType, Function<T, BindingData> bindingDataFn) {
    return create(
        BindingData.ofMap(
            map.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), bindingDataFn.apply(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
        literalType,
        map);
  }

  public static SdkBindingData<Map<String, String>> ofStringMap(Map<String, String> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.STRING)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(value))));
  }

  public static SdkBindingData<Map<String, Double>> ofFloatMap(Map<String, Double> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.FLOAT)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))));
  }

  public static SdkBindingData<Map<String, Long>> ofIntegerMap(Map<String, Long> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))));
  }

  public static SdkBindingData<Map<String, Boolean>> ofBooleanMap(Map<String, Boolean> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.BOOLEAN)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))));
  }

  public static SdkBindingData<Map<String, Duration>> ofDurationMap(Map<String, Duration> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.DURATION)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(value))));
  }

  public static SdkBindingData<Map<String, Instant>> ofDatetimeMap(Map<String, Instant> map) {
    return createMap(
        map,
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.DATETIME)),
        (value) -> BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(value))));
  }

  public static <T> SdkBindingData<List<T>> ofBindingCollection(
      LiteralType literalType, List<SdkBindingData<T>> elements) {
    List<BindingData> bindings = elements.stream().map(SdkBindingData::idl).collect(toList());
    BindingData bindingData = BindingData.ofCollection(bindings);

    boolean hasPromise = bindings.stream().anyMatch(SdkBindingData::isAPromise);
    List<T> unwrappedElements =
        hasPromise ? null : elements.stream().map(SdkBindingData::get).collect(toList());

    return SdkBindingData.create(bindingData, literalType, unwrappedElements);
  }

  private static boolean isAPromise(BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return false;
      case PROMISE:
        return true;
      case COLLECTION:
        return bindingData.collection().stream().anyMatch(SdkBindingData::isAPromise);
      case MAP:
        return bindingData.map().values().stream().anyMatch(SdkBindingData::isAPromise);
    }
    throw new IllegalArgumentException("BindingData.Kind not recognized: " + bindingData.kind());
  }

  public static <T> SdkBindingData<Map<String, T>> ofBindingMap(
      LiteralType literalType, Map<String, SdkBindingData<T>> valueMap) {

    Map<String, BindingData> bindings =
        valueMap.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().idl()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    BindingData bindingData = BindingData.ofMap(bindings);

    boolean hasPromise = bindings.values().stream().anyMatch(SdkBindingData::isAPromise);
    Map<String, T> unwrappedElements =
        hasPromise
            ? null
            : valueMap.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().get()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    return SdkBindingData.create(bindingData, literalType, unwrappedElements);
  }

  public static SdkBindingData<SdkStruct> ofStruct(SdkStruct value) {
    BindingData bindingData = BindingData.ofScalar(Scalar.ofGeneric(value.struct()));
    LiteralType literalType = LiteralType.ofSimpleType(SimpleType.STRUCT);
    return SdkBindingData.create(bindingData, literalType, value);
  }

  public static <T> SdkBindingData<T> ofOutputReference(
      String nodeId, String nodeVar, LiteralType type) {
    BindingData idl =
        BindingData.ofOutputReference(
            OutputReference.builder().nodeId(nodeId).var(nodeVar).build());
    // promises don't contain values yet
    return create(idl, type, null);
  }

  public T get() {
    if (idl().kind() == BindingData.Kind.PROMISE) {
      OutputReference promise = idl().promise();
      throw new IllegalArgumentException(
          String.format(
              "Value only available at workflow execution time: promise of %s[%s]",
              promise.nodeId(), promise.var()));
    }

    return value();
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
