/*
 * Copyright 2020-2023 Flyte Authors.
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

import java.time.Duration;
import java.time.Instant;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

/** Factory for {@link Literal}s. */
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

  static Literal ofBlob(Blob value) {
    return Literal.ofScalar(Scalar.ofBlob(value));
  }

  private static Literal ofPrimitive(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }
}
