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
package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;

class LiteralSerializer extends StdSerializer<Literal> {
  private static final long serialVersionUID = 0L;

  public LiteralSerializer() {
    super(Literal.class);
  }

  @Override
  public void serialize(Literal value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    switch (value.kind()) {
      case SCALAR:
        serialize(value.scalar(), gen, serializers);
        return;

      case MAP:
        gen.writeStartObject();
        for (Map.Entry<String, Literal> entry : value.map().entrySet()) {
          gen.writeFieldName(entry.getKey());
          serialize(entry.getValue(), gen, serializers);
        }
        gen.writeEndObject();

        return;

      case COLLECTION:
        gen.writeStartArray();
        for (Literal element : value.collection()) {
          serialize(element, gen, serializers);
        }
        gen.writeEndArray();

        return;
    }

    throw new AssertionError("Unexpected Literal.Kind: [" + value.kind() + "]");
  }

  public void serialize(Scalar value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    switch (value.kind()) {
      case PRIMITIVE:
        serialize(value.primitive(), gen);
        return;

      case GENERIC:
        serialize(value.generic(), gen);
        return;

      case BLOB:
        serializers.findValueSerializer(Blob.class).serialize(value.blob(), gen, serializers);
        return;
    }

    throw new AssertionError("Unexpected Scalar.Kind: [" + value.kind() + "]");
  }

  public void serialize(Primitive value, JsonGenerator gen) throws IOException {
    switch (value.kind()) {
      case BOOLEAN_VALUE:
        gen.writeBoolean(value.booleanValue());
        return;

      case DATETIME:
        gen.writeString(value.datetime().toString());
        return;

      case DURATION:
        gen.writeString(value.duration().toString());
        return;

      case FLOAT_VALUE:
        gen.writeNumber(value.floatValue());
        return;

      case INTEGER_VALUE:
        gen.writeNumber(value.integerValue());
        return;

      case STRING_VALUE:
        gen.writeString(value.stringValue());
        return;
    }

    throw new AssertionError("Unexpected Primitive.Kind: [" + value.kind() + "]");
  }

  private void serialize(Struct generic, JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    for (Map.Entry<String, Struct.Value> entry : generic.fields().entrySet()) {
      gen.writeFieldName(entry.getKey());
      serialize(entry.getValue(), gen);
    }

    gen.writeEndObject();
  }

  private void serialize(Struct.Value value, JsonGenerator gen) throws IOException {
    switch (value.kind()) {
      case BOOL_VALUE:
        gen.writeBoolean(value.boolValue());
        return;

      case LIST_VALUE:
        gen.writeStartArray();

        for (Struct.Value element : value.listValue()) {
          serialize(element, gen);
        }

        gen.writeEndArray();
        return;

      case NUMBER_VALUE:
        gen.writeNumber(value.numberValue());
        return;

      case STRING_VALUE:
        gen.writeString(value.stringValue());
        return;

      case STRUCT_VALUE:
        serialize(value.structValue(), gen);
        return;

      case NULL_VALUE:
        gen.writeNull();
        return;
    }

    throw new AssertionError("Unexpected Struct.Value.Kind: [" + value.kind() + "]");
  }
}
