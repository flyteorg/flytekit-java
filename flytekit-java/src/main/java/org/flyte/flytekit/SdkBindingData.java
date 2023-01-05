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

  public static <T> SdkBindingData<List<T>> ofCollection(List<T> collection,Function<T, SdkBindingData<T>> function) {
    return SdkBindingData.ofBindingCollection(collection.stream().map(function).collect(Collectors.toList()));
  }

  public static <T> SdkBindingData<Map<String, T>> ofMap(Map<String, T> map, Function<T, SdkBindingData<T>> bindingFunction) {
    return SdkBindingData.ofBindingMap(
        map.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), bindingFunction.apply(e.getValue())))
            .collect(
                toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue)));
  }

  public static SdkBindingData<Map<String, Long>> ofLongMap(Map<String, Long> map) {
    return ofMap(map, SdkBindingData::ofInteger);
  }

  public static SdkBindingData<Map<String, String>> ofStringMap(Map<String, String> map) {
    return ofMap(map, SdkBindingData::ofString);
  }

  public static SdkBindingData<Map<String, Boolean>> ofBooleanMap(Map<String, Boolean> map) {
    return ofMap(map, SdkBindingData::ofBoolean);
  }

  public static SdkBindingData<Map<String, Duration>> ofDurationMap(Map<String, Duration> map) {
    return ofMap(map, SdkBindingData::ofDuration);
  }

  public static SdkBindingData<Map<String, Instant>> ofDatetimeMap(Map<String, Instant> map) {
    return ofMap(map, SdkBindingData::ofDatetime);
  }


  public static <T> SdkBindingData<List<T>> ofBindingCollection(List<SdkBindingData<T>> elements) {
    // TODO we can fix that by introducing "top type" into type system and
    // implementing type casting in SDK, for now, we fail

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't create binding for an empty list without knowing the type, "
              + "to create an empty map use `of<type>Collection` instead");
    }

    List<BindingData> bindings = elements.stream().map(SdkBindingData::idl).collect(toList());
    BindingData bindingData = BindingData.ofCollection(bindings);

    LiteralType elementType = elements.get(0).type();
    LiteralType collectionType = LiteralType.ofCollectionType(elementType);
    boolean hasPromise = bindings.stream().anyMatch(SdkBindingData::isAPromise);
    List<T> unwrappedElements =
        hasPromise ? null : elements.stream().map(SdkBindingData::get).collect(toList());

    return SdkBindingData.create(bindingData, collectionType, unwrappedElements);
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
      Map<String, SdkBindingData<T>> valueMap) {
    // TODO we can fix that by introducing "top type" into type system and
    // implementing type casting in SDK, for now, we fail

    if (valueMap.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't create binding for an empty map without knowing the type, "
              + "to create an empty map use `of<type>Map` instead");
    }

    Map<String, BindingData> bindings =
        valueMap.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().idl()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    BindingData bindingData = BindingData.ofMap(bindings);

    LiteralType elementType = valueMap.values().iterator().next().type();
    LiteralType mapType = LiteralType.ofMapValueType(elementType);
    boolean hasPromise = bindings.values().stream().anyMatch(SdkBindingData::isAPromise);
    Map<String, T> unwrappedElements =
        hasPromise
            ? null
            : valueMap.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().get()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    return SdkBindingData.create(bindingData, mapType, unwrappedElements);
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
