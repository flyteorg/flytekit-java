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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;
import com.google.errorprone.annotations.InlineMe;
import java.time.Duration;
import java.time.Instant;

/** A simple value. Primitive can be: string, ... . */
@AutoOneOf(Primitive.Kind.class)
public abstract class Primitive {
  /**
   * Defines one of simple primitive types. These types will get translated into different
   * programming languages as described in
   * https://developers.google.com/protocol-buffers/docs/proto#scalar.
   */
  public enum Kind {
    INTEGER_VALUE,
    FLOAT_VALUE,
    STRING_VALUE,
    BOOLEAN_VALUE,
    DATETIME,
    DURATION
  }

  public abstract Kind kind();

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @deprecated Use {@link Primitive#kind()}.
   * @return simple type
   */
  @Deprecated
  public SimpleType type() {
    switch (kind()) {
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

    throw new AssertionError("Unexpected Primitive.Kind: " + kind());
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @deprecated Use {@link Primitive#integerValue()}.
   * @return integer value
   */
  @Deprecated
  @InlineMe(replacement = "this.integerValue()")
  public final long integer() {
    return integerValue();
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @deprecated Use {@link Primitive#floatValue()}.
   * @return float value
   */
  @Deprecated
  @InlineMe(replacement = "this.floatValue()")
  public final double float_() {
    return floatValue();
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @deprecated Use {@link Primitive#stringValue()}.
   * @return string value
   */
  @Deprecated
  @InlineMe(replacement = "this.stringValue()")
  public final String string() {
    return stringValue();
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @deprecated Use {@link Primitive#booleanValue()}.
   * @return boolean value
   */
  @Deprecated
  @InlineMe(replacement = "this.booleanValue()")
  public final boolean boolean_() {
    return booleanValue();
  }

  public abstract boolean booleanValue();

  public abstract long integerValue();

  public abstract double floatValue();

  public abstract String stringValue();

  public abstract Instant datetime();

  public abstract Duration duration();

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @param integer integer value
   * @deprecated Use {@link Primitive#ofIntegerValue(long)}.
   * @return primitive
   */
  @Deprecated
  @InlineMe(
      replacement = "Primitive.ofIntegerValue(integer)",
      imports = "org.flyte.api.v1.Primitive")
  public static Primitive ofInteger(long integer) {
    return ofIntegerValue(integer);
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @param float_ float value
   * @deprecated Use {@link Primitive#ofFloatValue(double)}.
   * @return primitive
   */
  @Deprecated
  @InlineMe(replacement = "Primitive.ofFloatValue(float_)", imports = "org.flyte.api.v1.Primitive")
  public static Primitive ofFloat(double float_) {
    return ofFloatValue(float_);
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @param string string value
   * @deprecated Use {@link Primitive#ofStringValue(String)}.
   * @return primitive
   */
  @Deprecated
  @InlineMe(replacement = "Primitive.ofStringValue(string)", imports = "org.flyte.api.v1.Primitive")
  public static Primitive ofString(String string) {
    return ofStringValue(string);
  }

  /**
   * Only for binary compatibility. To be removed in 0.3.x.
   *
   * @param booleanValue boolean value
   * @deprecated Use {@link Primitive#ofBooleanValue(boolean)}.
   * @return primitive
   */
  @Deprecated
  @InlineMe(
      replacement = "Primitive.ofBooleanValue(booleanValue)",
      imports = "org.flyte.api.v1.Primitive")
  public static Primitive ofBoolean(boolean booleanValue) {
    return ofBooleanValue(booleanValue);
  }

  public static Primitive ofFloatValue(double floatValue) {
    return AutoOneOf_Primitive.floatValue(floatValue);
  }

  public static Primitive ofIntegerValue(long integerValue) {
    return AutoOneOf_Primitive.integerValue(integerValue);
  }

  public static Primitive ofStringValue(String strinValue) {
    return AutoOneOf_Primitive.stringValue(strinValue);
  }

  public static Primitive ofBooleanValue(boolean booleanValue) {
    return AutoOneOf_Primitive.booleanValue(booleanValue);
  }

  public static Primitive ofDatetime(Instant datetime) {
    return AutoOneOf_Primitive.datetime(datetime);
  }

  public static Primitive ofDuration(Duration duration) {
    return AutoOneOf_Primitive.duration(duration);
  }
}
