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

import static java.util.Collections.unmodifiableMap;
import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

public class SdkLiteralTypes {

  public static SdkLiteralType<Long> integers() {
    return IntegerSdkLiteralType.INSTANCE;
  }

  public static SdkLiteralType<Double> floats() {
    return FloatSdkLiteralType.INSTANCE;
  }

  public static SdkLiteralType<String> strings() {
    return StringSdkLiteralType.INSTANCE;
  }

  public static SdkLiteralType<Boolean> booleans() {
    return BooleanSdkLiteralType.INSTANCE;
  }

  public static SdkLiteralType<Instant> datetimes() {
    return DatetimeSdkLiteralType.INSTANCE;
  }

  public static SdkLiteralType<Duration> durations() {
    return DurationSdkLiteralType.INSTANCE;
  }

  public static <T> SdkLiteralType<List<T>> collections(SdkLiteralType<T> elementType) {
    return new CollectionSdkLiteralType<>(elementType);
  }

  public static <T> SdkLiteralType<Map<String, T>> maps(SdkLiteralType<T> mapValueType) {
    return new MapSdkLiteralType<>(mapValueType);
  }

  private static class IntegerSdkLiteralType extends SdkLiteralType<Long> {
    private static final IntegerSdkLiteralType INSTANCE = new IntegerSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.INTEGER;
    }

    @Override
    public Literal toLiteral(Long value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value)));
    }

    @Override
    public Long fromLiteral(Literal literal) {
      return literal.scalar().primitive().integerValue();
    }
  }

  private static class FloatSdkLiteralType extends SdkLiteralType<Double> {
    private static final FloatSdkLiteralType INSTANCE = new FloatSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.FLOAT;
    }

    @Override
    public Literal toLiteral(Double value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value)));
    }

    @Override
    public Double fromLiteral(Literal literal) {
      return literal.scalar().primitive().floatValue();
    }
  }

  private static class StringSdkLiteralType extends SdkLiteralType<String> {
    private static final StringSdkLiteralType INSTANCE = new StringSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.STRING;
    }

    @Override
    public Literal toLiteral(String value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(value)));
    }

    @Override
    public String fromLiteral(Literal literal) {
      return literal.scalar().primitive().stringValue();
    }
  }

  private static class BooleanSdkLiteralType extends SdkLiteralType<Boolean> {
    private static final BooleanSdkLiteralType INSTANCE = new BooleanSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.BOOLEAN;
    }

    @Override
    public Literal toLiteral(Boolean value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value)));
    }

    @Override
    public Boolean fromLiteral(Literal literal) {
      return literal.scalar().primitive().booleanValue();
    }
  }

  private static class DatetimeSdkLiteralType extends SdkLiteralType<Instant> {
    private static final DatetimeSdkLiteralType INSTANCE = new DatetimeSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.DATETIME;
    }

    @Override
    public Literal toLiteral(Instant value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(value)));
    }

    @Override
    public Instant fromLiteral(Literal literal) {
      return literal.scalar().primitive().datetime();
    }
  }

  private static class DurationSdkLiteralType extends SdkLiteralType<Duration> {
    private static final DurationSdkLiteralType INSTANCE = new DurationSdkLiteralType();

    @Override
    public LiteralType getLiteralType() {
      return LiteralTypes.DURATION;
    }

    @Override
    public Literal toLiteral(Duration value) {
      return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(value)));
    }

    @Override
    public Duration fromLiteral(Literal literal) {
      return literal.scalar().primitive().duration();
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
  }

  private static class MapSdkLiteralType<T> extends SdkLiteralType<Map<String, T>> {
    private final SdkLiteralType<T> mapKeyType;

    private MapSdkLiteralType(SdkLiteralType<T> mapKeyType) {
      this.mapKeyType = mapKeyType;
    }

    @Override
    public LiteralType getLiteralType() {
      return LiteralType.ofMapValueType(mapKeyType.getLiteralType());
    }

    @Override
    public Literal toLiteral(Map<String, T> value) {
      Map<String, Literal> map = new LinkedHashMap<>();

      for (Map.Entry<String, T> entry : value.entrySet()) {
        map.put(entry.getKey(), mapKeyType.toLiteral(entry.getValue()));
      }

      return Literal.ofMap(unmodifiableMap(map));
    }

    @Override
    public Map<String, T> fromLiteral(Literal literal) {
      Map<String, T> map = new LinkedHashMap<>();

      for (Map.Entry<String, Literal> entry : literal.map().entrySet()) {
        map.put(entry.getKey(), mapKeyType.fromLiteral(entry.getValue()));
      }

      return unmodifiableMap(map);
    }
  }
}
