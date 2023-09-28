/*
 * Copyright 2023 Flyte Authors
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
package org.flyte.flytekit.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

public class LiteralSerializerFactory {
  public static LiteralSerializer create(
      String key,
      Literal value,
      JsonGenerator gen,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    switch (value.kind()) {
      case SCALAR:
        return createScalarSerializer(gen, key, value, serializerProvider, literalType);
      case MAP:
        return new MapSerializer(gen, key, value, serializerProvider, literalType);
      case COLLECTION:
        return new CollectionSerializer(gen, key, value, serializerProvider, literalType);
    }
    throw new AssertionError("Unexpected Literal.Kind: [" + value.kind() + "]");
  }

  private static ScalarSerializer createScalarSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    switch (value.scalar().kind()) {
      case PRIMITIVE:
        return createPrimitiveSerializer(gen, key, value, serializerProvider, literalType);
      case GENERIC:
        return new GenericSerializer(gen, key, value, serializerProvider, literalType);
      case BLOB:
        return new BlobSerializer(gen, key, value, serializerProvider, literalType);
    }
    throw new AssertionError("Unexpected Literal.Kind: [" + value.scalar().kind() + "]");
  }

  private static PrimitiveSerializer createPrimitiveSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    switch (value.scalar().primitive().kind()) {
      case INTEGER_VALUE:
        return new IntegerSerializer(gen, key, value, serializerProvider, literalType);
      case FLOAT_VALUE:
        return new FloatSerializer(gen, key, value, serializerProvider, literalType);
      case STRING_VALUE:
        return new StringSerializer(gen, key, value, serializerProvider, literalType);
      case BOOLEAN_VALUE:
        return new BooleanSerializer(gen, key, value, serializerProvider, literalType);
      case DATETIME:
        return new DatetimeSerializer(gen, key, value, serializerProvider, literalType);
      case DURATION:
        return new DurationSerializer(gen, key, value, serializerProvider, literalType);
    }
    throw new AssertionError(
        "Unexpected Primitive.Kind: [" + value.scalar().primitive().kind() + "]");
  }
}
