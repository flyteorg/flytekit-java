/*
 * Copyright 2020-2023 Flyte Authors
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

  static Literal ofInteger(long value) {
    return ofPrimitive(Primitive.ofIntegerValue(value));
  }

  static Literal ofFloat(double value) {
    return ofPrimitive(Primitive.ofFloatValue(value));
  }

  static Literal ofString(String value) {
    return ofPrimitive(Primitive.ofStringValue(value));
  }

  static Literal ofBoolean(boolean value) {
    return ofPrimitive(Primitive.ofBooleanValue(value));
  }

  static Literal ofDatetime(Instant value) {
    return ofPrimitive(Primitive.ofDatetime(value));
  }

  static Literal ofDuration(Duration value) {
    return ofPrimitive(Primitive.ofDuration(value));
  }

  private static Literal ofPrimitive(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

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
