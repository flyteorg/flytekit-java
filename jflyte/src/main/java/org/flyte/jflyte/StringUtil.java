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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

/** Utility to serialize between flytekit-api and string. */
class StringUtil {

  private StringUtil() {
    throw new UnsupportedOperationException();
  }

  static Map<String, String> serializeLiteralMap(Map<String, Literal> outputs) {
    Map<String, String> map = new HashMap<>();

    outputs.forEach((key, value) -> map.put(key, serialize(value)));

    return map;
  }

  private static String serialize(Literal value) {

    switch (value.kind()) {
      case SCALAR:
        return serialize(value.scalar());
      case COLLECTION:
        return serialize(value.collection());
      case MAP:
        return serialize(value.map());
    }

    throw new AssertionError("Unexpected Literal.Kind: " + value.kind());
  }

  private static String serialize(Scalar scalar) {

    switch (scalar.kind()) {
      case PRIMITIVE:
        return serialize(scalar.primitive());
    }

    throw new AssertionError("Unexpected Scalar.Kind: " + scalar.kind());
  }

  private static String serialize(List<Literal> literals) {
    List<String> list = new ArrayList<>();
    literals.forEach(literal -> list.add(serialize(literal)));
    return list.toString();
  }

  private static String serialize(Map<String, Literal> literals) {
    Map<String, String> map = new HashMap<>();
    literals.forEach((name, literal) -> map.put(name, serialize(literal)));
    return map.toString();
  }

  static String serialize(Primitive primitive) {

    switch (primitive.type()) {
      case INTEGER:
        return String.valueOf(primitive.integer());
      case FLOAT:
        return String.valueOf(primitive.float_());
      case STRING:
        return String.valueOf(primitive.string());
      case BOOLEAN:
        return String.valueOf(primitive.boolean_());
      case DATETIME:
        return String.valueOf(primitive.datetime());
      case DURATION:
        return String.valueOf(primitive.duration());
    }
    throw new AssertionError("Unexpected primitive type: " + primitive.type());
  }
}
