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

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.function.Function;
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
