/*
 * Copyright 2021-2023 Flyte Authors
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

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

/** A utility class for creating {@link SdkLiteralType} objects for different types. */
public class SdkLiteralTypes {

  private SdkLiteralTypes() {
    // prevent instantiation
  }

  /**
   * Returns a {@link SdkLiteralType} for the specified Java type.
   *
   * <dl>
   *   <dt>{@code Long.class} {@code ->}
   *   <dd>{@code SdkLiteralType<Long>}, equivalent to {@link #integers()}
   *   <dt>{@code Double.class} {@code ->}
   *   <dd>{@code SdkLiteralType<Double>}, equivalent to {@link #floats()}
   *   <dt>{@code String.class} {@code ->}
   *   <dd>{@code SdkLiteralType<String>}, equivalent to {@link #strings()}
   *   <dt>{@code Boolean.class} {@code ->}
   *   <dd>{@code SdkLiteralType<Boolean>}, equivalent to {@link #booleans()}
   *   <dt>{@code Instant.class} {@code ->}
   *   <dd>{@code SdkLiteralType<Instant>}, equivalent to {@link #datetimes()}
   *   <dt>{@code Duration.class} {@code ->}
   *   <dd>{@code SdkLiteralType<Duration>}, equivalent to {@link #durations()}
   * </dl>
   *
   * @param clazz Java type used to decide what {@link SdkLiteralType} to return.
   * @return the {@link SdkLiteralType} based on the java type
   * @param <T> type of the returned {@link SdkLiteralType}, matching the one specified.
   */
  @SuppressWarnings("unchecked")
  public static <T> SdkLiteralType<T> of(Class<T> clazz) {
    if (clazz.equals(Long.class)) {
      return (SdkLiteralType<T>) integers();
    } else if (clazz.equals(Double.class)) {
      return (SdkLiteralType<T>) floats();
    } else if (clazz.equals(String.class)) {
      return (SdkLiteralType<T>) strings();
    } else if (clazz.equals(Boolean.class)) {
      return (SdkLiteralType<T>) booleans();
    } else if (clazz.equals(Instant.class)) {
      return (SdkLiteralType<T>) datetimes();
    } else if (clazz.equals(Duration.class)) {
      return (SdkLiteralType<T>) durations();
    }
    throw new IllegalArgumentException("Unsupported type: " + clazz);
  }

  /**
   * Returns a {@link SdkLiteralType} for a collection of the specified Java type. Equivalent to
   * {code SdkLiteralTypes.collections(SdkLiteralTypes.of(elementsClass}
   *
   * @param elementsClass Java type used to decide what {@link SdkLiteralType} to return.
   * @return the {@link SdkLiteralType} based on the java type
   * @param <T> type of the elements of the collections for the returned {@link SdkLiteralType}.
   * @see SdkLiteralTypes#of
   */
  public static <T> SdkLiteralType<List<T>> collections(Class<T> elementsClass) {
    return collections(of(elementsClass));
  }

  /**
   * Returns a {@link SdkLiteralType} for a map of the specified Java type. Equivalent to {code
   * SdkLiteralTypes.maps(SdkLiteralTypes.of(valuesType}
   *
   * @param valuesType Java type used to decide what {@link SdkLiteralType} to return.
   * @return the {@link SdkLiteralType} based on the java type
   * @param <T> type of the values of the map for the returned {@link SdkLiteralType}. Key types are
   *     always {@code String}.
   * @see SdkLiteralTypes#of
   */
  public static <T> SdkLiteralType<Map<String, T>> maps(Class<T> valuesType) {
    return maps(of(valuesType));
  }

  /**
   * Returns a {@link SdkLiteralType} for flyte integers.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<Long> integers() {
    return IntegerSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for flyte floats.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<Double> floats() {
    return FloatSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for strings.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<String> strings() {
    return StringSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for booleans.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<Boolean> booleans() {
    return BooleanSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for flyte date times.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<Instant> datetimes() {
    return DatetimeSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for durations.
   *
   * @return the {@link SdkLiteralType}
   */
  public static SdkLiteralType<Duration> durations() {
    return DurationSdkLiteralType.INSTANCE;
  }

  /**
   * Returns a {@link SdkLiteralType} for flyte collections.
   *
   * @param elementType the {@link SdkLiteralType} representing the types of the elements of the
   *     collection.
   * @param <T> the Java type of the elements of the collection.
   * @return the {@link SdkLiteralType}
   */
  public static <T> SdkLiteralType<List<T>> collections(SdkLiteralType<T> elementType) {
    return new CollectionSdkLiteralType<>(elementType);
  }

  /**
   * Returns a {@link SdkLiteralType} for flyte maps.
   *
   * @param mapValueType the {@link SdkLiteralType} representing the types of the map's values.
   * @param <T> the Java type of the map's values, keys are always string.
   * @return the {@link SdkLiteralType}
   */
  public static <T> SdkLiteralType<Map<String, T>> maps(SdkLiteralType<T> mapValueType) {
    return new MapSdkLiteralType<>(mapValueType);
  }

  private static class IntegerSdkLiteralType extends PrimitiveSdkLiteralType<Long> {
    private static final IntegerSdkLiteralType INSTANCE = new IntegerSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.INTEGER;
    }

    @Override
    public Primitive toPrimitive(Long value) {
      return Primitive.ofIntegerValue(value);
    }

    @Override
    public Long fromPrimitive(Primitive primitive) {
      return primitive.integerValue();
    }

    @Override
    public String toString() {
      return "integers";
    }
  }

  private static class FloatSdkLiteralType extends PrimitiveSdkLiteralType<Double> {
    private static final FloatSdkLiteralType INSTANCE = new FloatSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.FLOAT;
    }

    @Override
    public Primitive toPrimitive(Double value) {
      return Primitive.ofFloatValue(value);
    }

    @Override
    public Double fromPrimitive(Primitive primitive) {
      return primitive.floatValue();
    }

    @Override
    public String toString() {
      return "floats";
    }
  }

  private static class StringSdkLiteralType extends PrimitiveSdkLiteralType<String> {
    private static final StringSdkLiteralType INSTANCE = new StringSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.STRING;
    }

    @Override
    public Primitive toPrimitive(String value) {
      return Primitive.ofStringValue(value);
    }

    @Override
    public String fromPrimitive(Primitive primitive) {
      return primitive.stringValue();
    }

    @Override
    public String toString() {
      return "strings";
    }
  }

  private static class BooleanSdkLiteralType extends PrimitiveSdkLiteralType<Boolean> {
    private static final BooleanSdkLiteralType INSTANCE = new BooleanSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.BOOLEAN;
    }

    @Override
    public Primitive toPrimitive(Boolean value) {
      return Primitive.ofBooleanValue(value);
    }

    @Override
    public Boolean fromPrimitive(Primitive primitive) {
      return primitive.booleanValue();
    }

    @Override
    public String toString() {
      return "booleans";
    }
  }

  private static class DatetimeSdkLiteralType extends PrimitiveSdkLiteralType<Instant> {
    private static final DatetimeSdkLiteralType INSTANCE = new DatetimeSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.DATETIME;
    }

    @Override
    public Primitive toPrimitive(Instant value) {
      return Primitive.ofDatetime(value);
    }

    @Override
    public Instant fromPrimitive(Primitive primitive) {
      return primitive.datetime();
    }

    @Override
    public String toString() {
      return "datetimes";
    }
  }

  private static class DurationSdkLiteralType extends PrimitiveSdkLiteralType<Duration> {
    private static final DurationSdkLiteralType INSTANCE = new DurationSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.DURATION;
    }

    @Override
    public Primitive toPrimitive(Duration value) {
      return Primitive.ofDuration(value);
    }

    @Override
    public Duration fromPrimitive(Primitive primitive) {
      return primitive.duration();
    }

    @Override
    public String toString() {
      return "durations";
    }
  }

  private static class CollectionSdkLiteralType<T> extends SdkLiteralType<List<T>> {
    private final SdkLiteralType<T> elementType;

    private CollectionSdkLiteralType(SdkLiteralType<T> elementType) {
      this.elementType = elementType;
    }

    @Override
    public LiteralType getLiteralType() {
      return LiteralType.ofCollectionType(elementType.getLiteralType());
    }

    @Override
    public Literal toLiteral(List<T> value) {
      List<Literal> collection =
          value.stream().map(elementType::toLiteral).collect(toUnmodifiableList());

      return Literal.ofCollection(collection);
    }

    @Override
    public List<T> fromLiteral(Literal literal) {
      return literal.collection().stream()
          .map(elementType::fromLiteral)
          .collect(toUnmodifiableList());
    }

    @Override
    public BindingData toBindingData(List<T> value) {
      return BindingData.ofCollection(
          value.stream().map(elementType::toBindingData).collect(toUnmodifiableList()));
    }

    @Override
    public String toString() {
      return "collections of [" + elementType + ']';
    }
  }

  private static class MapSdkLiteralType<T> extends SdkLiteralType<Map<String, T>> {
    private final SdkLiteralType<T> valuesType;

    private MapSdkLiteralType(SdkLiteralType<T> valuesType) {
      this.valuesType = valuesType;
    }

    @Override
    public LiteralType getLiteralType() {
      return LiteralType.ofMapValueType(valuesType.getLiteralType());
    }

    @Override
    public Literal toLiteral(java.util.Map<String, T> value) {
      var map =
          value.entrySet().stream()
              .collect(toUnmodifiableMap(Entry::getKey, e -> valuesType.toLiteral(e.getValue())));

      return Literal.ofMap(map);
    }

    @Override
    public java.util.Map<String, T> fromLiteral(Literal literal) {
      return literal.map().entrySet().stream()
          .collect(toUnmodifiableMap(Entry::getKey, e -> valuesType.fromLiteral(e.getValue())));
    }

    @Override
    public BindingData toBindingData(java.util.Map<String, T> value) {
      return BindingData.ofMap(
          value.entrySet().stream()
              .collect(
                  toUnmodifiableMap(Entry::getKey, e -> valuesType.toBindingData(e.getValue()))));
    }

    @Override
    public String toString() {
      return "map of [" + valuesType + ']';
    }
  }

  private abstract static class PrimitiveSdkLiteralType<T> extends SdkLiteralType<T> {

    @Override
    public final Literal toLiteral(T value) {
      return Literal.ofScalar(Scalar.ofPrimitive(toPrimitive(value)));
    }

    public abstract Primitive toPrimitive(T value);

    @Override
    public final T fromLiteral(Literal literal) {
      return fromPrimitive(literal.scalar().primitive());
    }

    public abstract T fromPrimitive(Primitive primitive);

    @Override
    public final BindingData toBindingData(T value) {
      return BindingData.ofScalar(Scalar.ofPrimitive(toPrimitive(value)));
    }
  }
}
