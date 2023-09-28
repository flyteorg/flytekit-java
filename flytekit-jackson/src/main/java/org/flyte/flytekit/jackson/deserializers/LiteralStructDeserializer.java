/*
 * Copyright 2023 Flyte Authors.
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
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;

public class LiteralStructDeserializer extends StdDeserializer<Literal> {
  private static final long serialVersionUID = -6835948754469626304L;

  public LiteralStructDeserializer() {
    super(Literal.class);
  }

  @Override
  public Literal deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

    Struct generic = readValueAsStruct(p);
    return Literal.ofScalar(Scalar.ofGeneric(generic));
  }

  private static Struct readValueAsStruct(JsonParser p) throws IOException {
    verifyToken(p, JsonToken.START_OBJECT);
    p.nextToken();

    Map<String, Value> fields = new HashMap<>();

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

        List<Value> valuesList = new ArrayList<>();

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
}
