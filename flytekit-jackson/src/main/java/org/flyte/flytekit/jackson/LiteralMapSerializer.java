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
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Primitive.Kind;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;

class LiteralMapSerializer extends StdSerializer<JacksonLiteralMap> {
  private static final long serialVersionUID = 0L;

  public LiteralMapSerializer() {
    super(JacksonLiteralMap.class);
  }

  @Override
  public void serialize(JacksonLiteralMap map, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    for (Map.Entry<String, Literal> entry : map.getLiteralMap().entrySet()) {
      gen.writeFieldName(entry.getKey());
      gen.writeStartObject();
      serializeLiteral(entry.getKey(), entry.getValue(), gen, serializers, map.getLiteralTypeMap());
      gen.writeEndObject();
    }
    gen.writeEndObject();
  }


  public void serializeLiteral(
      String key,
      Literal value,
      JsonGenerator gen,
      SerializerProvider serializers,
      Map<String, LiteralType> literalTypeMap)
      throws IOException {

    switch (value.kind()) {
      case SCALAR:
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.SCALAR);
        serializeScalar(value.scalar(), gen, serializers, literalTypeMap);
        return;

      case MAP:
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.MAP);
        gen.writeFieldName("type");
        gen.writeObject(literalTypeMap.get(key).mapValueType().simpleType());
        gen.writeFieldName("value");
        gen.writeStartObject();

        for (Map.Entry<String, Literal> entry : value.map().entrySet()) {
          gen.writeFieldName(entry.getKey());
          gen.writeStartObject();
          serializeLiteral(entry.getKey(), entry.getValue(), gen, serializers, literalTypeMap);
          gen.writeEndObject();
        }

        gen.writeEndObject();
        return;

      case COLLECTION:
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.COLLECTION);
        gen.writeFieldName("type");
        gen.writeObject(literalTypeMap.get(key).collectionType().simpleType());
        gen.writeFieldName("value");
        gen.writeStartArray();

        for (Literal element : value.collection()) {
          gen.writeStartObject();
          serializeLiteral(key, element, gen, serializers, literalTypeMap);
          gen.writeEndObject();
        }

        gen.writeEndArray();
        return;
    }

    throw new AssertionError("Unexpected Literal.Kind: [" + value.kind() + "]");
  }

  public void serializeScalar(Scalar value, JsonGenerator gen, SerializerProvider serializers, Map<String, LiteralType> literalTypeMap)
      throws IOException {
    switch (value.kind()) {
      case PRIMITIVE:
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.PRIMITIVE);
        serializePrimitive(value.primitive(), gen);
        return;

      case GENERIC:
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.GENERIC);
        serializeGeneric(value.generic(), gen, serializers, literalTypeMap);
        return;

      case BLOB:
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.BLOB);
        serializers.findValueSerializer(Blob.class).serialize(value.blob(), gen, serializers);
        return;
    }

    throw new AssertionError("Unexpected Scalar.Kind: [" + value.kind() + "]");
  }

  public void serializePrimitive(Primitive value, JsonGenerator gen) throws IOException {
    switch (value.kind()) {
      case BOOLEAN_VALUE:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.BOOLEAN_VALUE);
        gen.writeFieldName("value");
        gen.writeBoolean(value.booleanValue());
        return;

      case DATETIME:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.DATETIME);
        gen.writeFieldName("value");
        gen.writeString(value.datetime().toString());
        return;

      case DURATION:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.DURATION);
        gen.writeFieldName("value");
        gen.writeString(value.duration().toString());
        return;

      case FLOAT_VALUE:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.FLOAT_VALUE);
        gen.writeFieldName("value");
        gen.writeNumber(value.floatValue());
        return;

      case INTEGER_VALUE:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.INTEGER_VALUE);
        gen.writeFieldName("value");
        gen.writeNumber(value.integerValue());
        return;

      case STRING_VALUE:
        gen.writeFieldName("primitive");
        gen.writeObject(Kind.STRING_VALUE);
        gen.writeFieldName("value");
        gen.writeString(value.stringValue());
        return;
    }

    throw new AssertionError("Unexpected Primitive.Kind: [" + value.kind() + "]");
  }

  private void serializeGeneric(Struct generic, JsonGenerator gen, SerializerProvider serializers, Map<String, LiteralType> literalTypeMap) throws IOException {
    for (Map.Entry<String, Value> entry : generic.fields().entrySet()) {
      gen.writeFieldName(entry.getKey());
      serializeStructValue(entry.getValue(), gen, serializers, literalTypeMap);
    }
  }

  private void serializeStructValue(Value value, JsonGenerator gen, SerializerProvider serializers, Map<String, LiteralType> literalTypeMap) throws IOException {
    switch (value.kind()) {
      case BOOL_VALUE:
        gen.writeStartObject();
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.SCALAR);
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.GENERIC);
        gen.writeFieldName("structType");
        gen.writeObject(Value.Kind.BOOL_VALUE);
        gen.writeFieldName("structValue");
        gen.writeBoolean(value.boolValue());
        gen.writeEndObject();

        return;

      case LIST_VALUE:
        throw new RuntimeException("not supported list inside the struct");

      case NUMBER_VALUE:
        gen.writeStartObject();
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.SCALAR);
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.GENERIC);
        gen.writeFieldName("structType");
        gen.writeObject(Value.Kind.NUMBER_VALUE);
        gen.writeFieldName("structValue");
        gen.writeNumber(value.numberValue());
        gen.writeEndObject();
        return;

      case STRING_VALUE:
        gen.writeStartObject();
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.SCALAR);
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.GENERIC);
        gen.writeFieldName("structType");
        gen.writeObject(Value.Kind.STRING_VALUE);
        gen.writeFieldName("structValue");
        gen.writeString(value.stringValue());
        gen.writeEndObject();
        return;

      case STRUCT_VALUE:
        gen.writeStartObject();
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.SCALAR);
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.GENERIC);
        serializeGeneric(value.structValue(), gen, serializers, literalTypeMap);
        gen.writeEndObject();
        return;

      case NULL_VALUE:
        gen.writeNull();
        return;
    }

    throw new AssertionError("Unexpected Struct.Value.Kind: [" + value.kind() + "]");
  }
}
