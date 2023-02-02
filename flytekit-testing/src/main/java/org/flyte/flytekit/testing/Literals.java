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
package org.flyte.flytekit.testing;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

/** Utility to create {@link Literal} from raw values. */
class Literals {

  /**
   * Create a {@link Literal} from Integer
   *
   * @param value Long value
   * @return Literal value
   */
  static Literal ofInteger(long value) {
    return ofPrimitive(Primitive.ofIntegerValue(value));
  }

  /**
   * Create a {@link Literal} from Float
   *
   * @param value Double value
   * @return Literal value
   */
  static Literal ofFloat(double value) {
    return ofPrimitive(Primitive.ofFloatValue(value));
  }

  /**
   * Create a {@link Literal} from String
   *
   * @param value String value
   * @return Literal value
   */
  static Literal ofString(String value) {
    return ofPrimitive(Primitive.ofStringValue(value));
  }

  /**
   * Create a {@link Literal} from Boolean
   *
   * @param value Boolean value
   * @return Literal value
   */
  static Literal ofBoolean(boolean value) {
    return ofPrimitive(Primitive.ofBooleanValue(value));
  }

  /**
   * Create a {@link Literal} from Datetime
   *
   * @param value Instant value
   * @return Literal value
   */
  static Literal ofDatetime(Instant value) {
    return ofPrimitive(Primitive.ofDatetime(value));
  }

  /**
   * Create a {@link Literal} from Duration
   *
   * @param value Duration value
   * @return Literal value
   */
  static Literal ofDuration(Duration value) {
    return ofPrimitive(Primitive.ofDuration(value));
  }

  /**
   * Create a {@link Literal} from Primitive
   *
   * @param primitive Primitive value
   * @return Literal value
   */
  private static Literal ofPrimitive(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  /**
   * Transform from {@link Literal} to {@link BindingData}
   *
   * @param literal Literal value
   * @return BindingData value
   */
  static BindingData toBindingData(Literal literal) {
    switch (literal.kind()) {
      case SCALAR:
        return BindingData.ofScalar(literal.scalar());

      case MAP:
        Map<String, BindingData> bindingMap =
            literal.map().entrySet().stream()
                .collect(
                    collectingAndThen(
                        toMap(Map.Entry::getKey, x -> toBindingData(x.getValue())),
                        Collections::unmodifiableMap));

        return BindingData.ofMap(bindingMap);

      case COLLECTION:
        List<BindingData> bindingCollection =
            literal.collection().stream()
                .map(Literals::toBindingData)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));

        return BindingData.ofCollection(bindingCollection);
    }

    throw new AssertionError("Unexpected Literal.Kind: " + literal.kind());
  }
}
