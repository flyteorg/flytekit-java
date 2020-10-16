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
package org.flyte.flytekit.jackson;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
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
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;

class LiteralMapDeserializer extends StdDeserializer<JacksonLiteralMap> {
  private static final long serialVersionUID = 0L;

  private final transient Map<String, LiteralType> literalTypeMap;

  public LiteralMapDeserializer(Map<String, LiteralType> literalTypeMap) {
    super(JacksonLiteralMap.class);

    this.literalTypeMap = literalTypeMap;
  }

  @Override
  public JacksonLiteralMap deserialize(JsonParser p, DeserializationContext ctxt)
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
      literalMap.put(fieldName, deserialize(p, literalType));

      p.nextToken();
    }

    return new JacksonLiteralMap(unmodifiableMap(literalMap));
  }

  private static Literal deserialize(JsonParser p, LiteralType literalType) throws IOException {
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        return deserialize(p, literalType.simpleType());

      case COLLECTION_TYPE:
        List<Literal> collection = new ArrayList<>();

        verifyToken(p, JsonToken.START_ARRAY);

        while (p.nextToken() != JsonToken.END_ARRAY) {
          collection.add(deserialize(p, literalType.collectionType()));
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
          map.put(fieldName, deserialize(p, literalType.mapValueType()));

          p.nextToken();
        }

        return Literal.ofMap(unmodifiableMap(map));

      case SCHEMA_TYPE:
      case BLOB_TYPE:
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

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString(stringValue)));

      case INTEGER:
        Long integerValue = p.readValueAs(Long.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(integerValue)));

      case DURATION:
        Duration durationValue = p.readValueAs(Duration.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(durationValue)));

      case DATETIME:
        Instant datetimeValue = p.readValueAs(Instant.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(datetimeValue)));

      case FLOAT:
        Double floatValue = p.readValueAs(Double.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(floatValue)));

      case BOOLEAN:
        Boolean booleanValue = p.readValueAs(Boolean.class);

        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBoolean(booleanValue)));
    }

    throw new AssertionError(String.format("Unexpected SimpleType: [%s]", simpleType));
  }

  private static void verifyToken(JsonParser p, JsonToken token) {
    if (p.currentToken() != token) {
      throw new IllegalStateException(
          String.format("Unexpected token [%s], expected [%s]", p.currentToken(), token));
    }
  }

  // base class is serializable, but we can't serialize literalTypeMap, so throw exception instead
  private void readObject(ObjectInputStream stream) throws NotSerializableException {
    throw new NotSerializableException(getClass().getName());
  }
}
