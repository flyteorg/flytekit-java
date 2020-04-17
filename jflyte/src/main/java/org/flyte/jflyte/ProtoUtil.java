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
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

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

  static Primitive deserialize(Literals.Primitive primitive) {
    if (primitive.getStringValue() != null) {
      return Primitive.of(primitive.getStringValue());
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }
}
