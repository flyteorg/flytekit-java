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
package org.flyte.flytekit.jackson.deserializers;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.flyte.flytekit.jackson.deserializers.JsonTokenUtil.verifyToken;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.flytekit.jackson.JacksonLiteralMap;

public class LiteralMapDeserializer extends StdDeserializer<JacksonLiteralMap> {
  private static final long serialVersionUID = 0L;

  private final transient Map<String, LiteralType> literalTypeMap;

  public LiteralMapDeserializer(Map<String, LiteralType> literalTypeMap) {
    super(JacksonLiteralMap.class);

    this.literalTypeMap = literalTypeMap;
  }

  @Override
  public JacksonLiteralMap deserialize(JsonParser p, DeserializationContext ctx)
      throws IOException {
    Map<String, Literal> literalMap = new HashMap<>();

    verifyToken(p, JsonToken.START_OBJECT);
    p.nextToken();

    while (p.currentToken() != JsonToken.END_OBJECT) {
      verifyToken(p, JsonToken.FIELD_NAME);
      String fieldName = p.currentName();

      LiteralType literalType = literalTypeMap.get(fieldName);

      if (literalType == null) {
        throw new IllegalStateException("Unexpected field [" + fieldName + "]");
      }

      p.nextToken();
      literalMap.put(fieldName, deserialize(p, ctx, literalType));

      p.nextToken();
    }

    return new JacksonLiteralMap(unmodifiableMap(literalMap), unmodifiableMap(literalTypeMap));
  }

  private static Literal deserialize(
      JsonParser p, DeserializationContext ctx, LiteralType literalType) throws IOException {
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        return deserialize(p, literalType.simpleType());

      case COLLECTION_TYPE:
        List<Literal> collection = new ArrayList<>();

        verifyToken(p, JsonToken.START_ARRAY);

        while (p.nextToken() != JsonToken.END_ARRAY) {
          collection.add(deserialize(p, ctx, literalType.collectionType()));
        }

        return Literal.ofCollection(unmodifiableList(collection));

      case MAP_VALUE_TYPE:
        Map<String, Literal> map = new HashMap<>();

        verifyToken(p, JsonToken.START_OBJECT);
        p.nextToken();

        while (p.currentToken() != JsonToken.END_OBJECT) {
          verifyToken(p, JsonToken.FIELD_NAME);
          String fieldName = p.currentName();

          p.nextToken();
          map.put(fieldName, deserialize(p, ctx, literalType.mapValueType()));

          p.nextToken();
        }

        return Literal.ofMap(unmodifiableMap(map));

      case BLOB_TYPE:
        JavaType type = ctx.constructType(Blob.class);
        Blob blob = (Blob) ctx.findNonContextualValueDeserializer(type).deserialize(p, ctx);

        return Literal.ofScalar(Scalar.ofBlob(blob));

      case SCHEMA_TYPE:
        throw new IllegalArgumentException(
            String.format("Unsupported LiteralType.Kind: [%s]", literalType.getKind()));
    }

    throw new AssertionError(
        String.format("Unexpected LiteralType.Kind: [%s]", literalType.getKind()));
  }

  private static Literal deserialize(JsonParser p, SimpleType simpleType) throws IOException {
    switch (simpleType) {
      case STRING:
        String stringValue = p.readValueAs(String.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(stringValue)));

      case INTEGER:
        Long integerValue = p.readValueAs(Long.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(integerValue)));

      case DURATION:
        Duration durationValue = p.readValueAs(Duration.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(durationValue)));

      case DATETIME:
        Instant datetimeValue = p.readValueAs(Instant.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(datetimeValue)));

      case FLOAT:
        Double floatValue = p.readValueAs(Double.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(floatValue)));

      case BOOLEAN:
        Boolean booleanValue = p.readValueAs(Boolean.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(booleanValue)));

      case STRUCT:
        Struct generic = readValueAsStruct(p);

        return Literal.ofScalar(Scalar.ofGeneric(generic));
    }

    throw new AssertionError(String.format("Unexpected SimpleType: [%s]", simpleType));
  }

  private static Struct readValueAsStruct(JsonParser p) throws IOException {
    verifyToken(p, JsonToken.START_OBJECT);
    p.nextToken();

    Map<String, Struct.Value> fields = new HashMap<>();

    while (p.currentToken() != JsonToken.END_OBJECT) {
      verifyToken(p, JsonToken.FIELD_NAME);
      String fieldName = p.currentName();
      p.nextToken();

      fields.put(fieldName, readValueAsStructValue(p));

      p.nextToken();
    }

    return Struct.of(unmodifiableMap(fields));
  }

  private static Struct.Value readValueAsStructValue(JsonParser p) throws IOException {
    switch (p.currentToken()) {
      case START_ARRAY:
        p.nextToken();

        List<Struct.Value> valuesList = new ArrayList<>();

        while (p.currentToken() != JsonToken.END_ARRAY) {
          Struct.Value value = readValueAsStructValue(p);
          p.nextToken();

          valuesList.add(value);
        }

        return Struct.Value.ofListValue(unmodifiableList(valuesList));

      case START_OBJECT:
        Struct struct = readValueAsStruct(p);

        return Struct.Value.ofStructValue(struct);

      case VALUE_STRING:
        String stringValue = p.readValueAs(String.class);

        return Struct.Value.ofStringValue(stringValue);

      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
        Double doubleValue = p.readValueAs(Double.class);

        return Struct.Value.ofNumberValue(doubleValue);

      case VALUE_NULL:
        return Struct.Value.ofNullValue();

      case VALUE_FALSE:
        return Struct.Value.ofBoolValue(false);

      case VALUE_TRUE:
        return Struct.Value.ofBoolValue(true);

      case FIELD_NAME:
      case NOT_AVAILABLE:
      case VALUE_EMBEDDED_OBJECT:
      case END_ARRAY:
      case END_OBJECT:
        throw new IllegalStateException("Unexpected token: " + p.currentToken());
    }

    throw new AssertionError("Unexpected token: " + p.currentToken());
  }

  // base class is serializable, but we can't serialize literalTypeMap, so throw exception instead
  private void readObject(ObjectInputStream stream) throws NotSerializableException {
    throw new NotSerializableException(getClass().getName());
  }
}
