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
import org.flyte.api.v1.BindingData.Kind;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;

/** Specifies either a simple value or a reference to another output. */
@AutoValue
public abstract class SdkBindingData<T> {

  abstract BindingData idl();

  abstract LiteralType type();

  @Nullable
  abstract T value();

  // TODO: it would be interesting to see if we can use java 9 modules to only expose this method
  //      to other modules in the sdk
  /**
   * Creates a {@code SdkBindingData} based on its components; however it is not meant to be used by
   * users directly, but users must use the higher level factory methods.
   *
   * @param idl the api class equivalent to this
   * @param type the SdkBindingData type
   * @param value when {@code idl} is not a {@link BindingData.Kind#PROMISE} then value contains the
   *     simple value of this class, must be null otherwise
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<T> create(BindingData idl, LiteralType type, @Nullable T value) {
    if (idl.kind().equals(Kind.PROMISE) && value == null) {
      throw new IllegalArgumentException(
          "SdkBindingData is not a promise and therefore value couldn't be null");
    }
    return new AutoValue_SdkBindingData<>(idl, type, value);
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

    return create(bindingData, literalType, value);
  }

  // TODO: ofCollection and ofMap receive a literal type for itself, it would be simpler if they
  //  receive the element literal type instead
  /**
   * Creates a {@code SdkBindingData} for a flyte collection given a java {@code List<T>} and a
   * function to know how to convert each element form such list to a {@code SdkBindingData}.
   *
   * @param collection collection to represent on this data.
   * @param literalType literal type for the whole collection. It must be a {@link
   *     LiteralType.Kind#COLLECTION_TYPE}.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<List<T>> ofCollection(
      List<T> collection, LiteralType literalType, Function<T, SdkBindingData<T>> mapper) {
    return SdkBindingData.ofBindingCollection(
        literalType, collection.stream().map(mapper).collect(Collectors.toList()));
  }

  private static <T> SdkBindingData<List<T>> createCollection(
      List<T> collection, LiteralType literalType, Function<T, BindingData> bindingDataFn) {
    return create(
        BindingData.ofCollection(
            collection.stream().map(bindingDataFn).collect(Collectors.toList())),
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
   * @param map map to represent on this data.
   * @param literalType literal type for the whole collection. It must be a {@link
   *     LiteralType.Kind#MAP_VALUE_TYPE}.
   * @return the new {@code SdkBindingData}
   */
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

  // TODO: ordering of parameters is inconsistent with other methods here
  /**
   * Creates a {@code SdkBindingData} for a flyte collection given a java {@code
   * List<SdkBindingData<T>>} and a literalType tp be used.
   *
   * @param elements collection to represent on this data.
   * @param literalType literal type for the whole collection. It must be a {@link
   *     LiteralType.Kind#COLLECTION_TYPE}.
   * @return the new {@code SdkBindingData}
   */
  public static <T> SdkBindingData<List<T>> ofBindingCollection(
      LiteralType literalType, List<SdkBindingData<T>> elements) {
    List<BindingData> bindings = elements.stream().map(SdkBindingData::idl).collect(toList());
    BindingData bindingData = BindingData.ofCollection(bindings);

    checkIncompatibleTypes(literalType.collectionType(), elements);
    boolean hasPromise = bindings.stream().anyMatch(SdkBindingData::isAPromise);
    List<T> unwrappedElements =
        hasPromise ? null : elements.stream().map(SdkBindingData::get).collect(toList());

    return SdkBindingData.create(bindingData, literalType, unwrappedElements);
  }

  private static <T> void checkIncompatibleTypes(
      LiteralType literalType, List<SdkBindingData<T>> elements) {
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
        return bindingData.collection().stream().anyMatch(SdkBindingData::isAPromise);
      case MAP:
        return bindingData.map().values().stream().anyMatch(SdkBindingData::isAPromise);
    }
    throw new IllegalArgumentException("BindingData.Kind not recognized: " + bindingData.kind());
  }

  /**
   * Creates a {@code SdkBindingData} for a flyte map given a java {@code Map<String,
   * SdkBindingData<T>>} and a literalType tp be used.
   *
   * @param valueMap collection to represent on this data.
   * @param literalType literal type for the whole map. It must be a {@link
   *     LiteralType.Kind#MAP_VALUE_TYPE}.
   * @return the new {@code SdkBindingData}
   */
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
    return create(idl, type, null);
  }

  /**
   * Returns the simple value contained by this data.
   *
   * @return the value that this simple data holds
   * @throws IllegalArgumentException when this data is an output reference
   */
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
