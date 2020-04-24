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
package org.flyte.jflyte;

import flyteidl.core.Literals;
import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Timestamp;

class ProtoUtil {

  static Map<String, Literal> deserialize(Literals.LiteralMap literalMap) {
    Map<String, Literal> inputs = new HashMap<>();

    for (Map.Entry<String, Literals.Literal> entry : literalMap.getLiteralsMap().entrySet()) {
      inputs.put(entry.getKey(), deserialize(entry.getValue()));
    }

    return inputs;
  }

  static Literal deserialize(Literals.Literal literal) {
    if (literal.getScalar() != null) {
      return Literal.create(deserialize(literal.getScalar()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
  }

  static Scalar deserialize(Literals.Scalar scalar) {
    if (scalar.getPrimitive() != null) {
      return Scalar.create(deserialize(scalar.getPrimitive()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Scalar [%s]", scalar));
  }

  @SuppressWarnings("fallthrough")
  static Primitive deserialize(Literals.Primitive primitive) {
    switch (primitive.getValueCase()) {
      case INTEGER:
        return Primitive.of(primitive.getInteger());
      case FLOAT_VALUE:
        return Primitive.of(primitive.getFloatValue());
      case STRING_VALUE:
        return Primitive.of(primitive.getStringValue());
      case BOOLEAN:
        return Primitive.of(primitive.getBoolean());
      case DATETIME:
        com.google.protobuf.Timestamp datetime = primitive.getDatetime();
        return Primitive.of(Timestamp.create(datetime.getSeconds(), datetime.getNanos()));
      case DURATION:
        com.google.protobuf.Duration duration = primitive.getDuration();
        return Primitive.of(Duration.create(duration.getSeconds(), duration.getNanos()));
      case VALUE_NOT_SET:
        // fallthrough
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }
}
