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

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

// TODO: this class it is not used. We should remove it or even better use it in place of
//  raw literal types in SdkBinding data
public class SdkLiteralTypes {

  private SdkLiteralTypes() {
    // prevent instantiation
  }

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

    @Override
    public BindingData toBindingData(Long value) {
      return ofPrimitive(Primitive::ofIntegerValue, value);
    }

    @Override
    public String toString() {
      return "integers";
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

    @Override
    public BindingData toBindingData(Double value) {
      return ofPrimitive(Primitive::ofFloatValue, value);
    }

    @Override
    public String toString() {
      return "floats";
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

    @Override
    public BindingData toBindingData(String value) {
      return ofPrimitive(Primitive::ofStringValue, value);
    }

    @Override
    public String toString() {
      return "strings";
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

    @Override
    public BindingData toBindingData(Boolean value) {
      return BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value)));
    }

    @Override
    public String toString() {
      return "booleans";
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

    @Override
    public BindingData toBindingData(Instant value) {
      return ofPrimitive(Primitive::ofDatetime, value);
    }

    @Override
    public String toString() {
      return "datetimes";
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

    @Override
    public BindingData toBindingData(Duration value) {
      return ofPrimitive(Primitive::ofDuration, value);
    }

    @Override
    public String toString() {
      return "durations";
    }
  }

  private static class CollectionSdkLiteralType<T> extends SdkCollectionLiteralType<T> {
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

    @Override
    public SdkLiteralType<T> getElementType() {
      return elementType;
    }
  }

  private static class MapSdkLiteralType<T> extends SdkMapLiteralType<T> {
    private final SdkLiteralType<T> valuesType;

    private MapSdkLiteralType(SdkLiteralType<T> valuesType) {
      this.valuesType = valuesType;
    }

    @Override
    public LiteralType getLiteralType() {
      return LiteralType.ofMapValueType(valuesType.getLiteralType());
    }

    @Override
    public Literal toLiteral(Map<String, T> value) {
      Map<String, Literal> map = new LinkedHashMap<>();

      for (Map.Entry<String, T> entry : value.entrySet()) {
        map.put(entry.getKey(), valuesType.toLiteral(entry.getValue()));
      }

      return Literal.ofMap(unmodifiableMap(map));
    }

    @Override
    public Map<String, T> fromLiteral(Literal literal) {
      Map<String, T> map = new LinkedHashMap<>();

      for (Map.Entry<String, Literal> entry : literal.map().entrySet()) {
        map.put(entry.getKey(), valuesType.fromLiteral(entry.getValue()));
      }

      return unmodifiableMap(map);
    }

    @Override
    public BindingData toBindingData(Map<String, T> value) {
      return BindingData.ofMap(
          value.entrySet().stream()
              .map(entry -> Map.entry(entry.getKey(), valuesType.toBindingData(entry.getValue())))
              .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
      return "map of [" + valuesType + ']';
    }

    @Override
    public SdkLiteralType<T> getValuesType() {
      return valuesType;
    }
  }

  private static <T> BindingData ofPrimitive(Function<T, Primitive> toPrimitive, T value) {
    var primitive = toPrimitive.apply(value);
    return BindingData.ofScalar(Scalar.ofPrimitive(primitive));
  }
}
