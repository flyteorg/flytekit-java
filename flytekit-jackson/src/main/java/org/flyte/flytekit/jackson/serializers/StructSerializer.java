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
package org.flyte.flytekit.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;
import org.flyte.api.v1.Struct.Value.Kind;

public class StructSerializer extends StdSerializer<Struct> {
  private static final long serialVersionUID = -3155214696984949940L;

  public StructSerializer() {
    super(Struct.class);
  }

  @Override
  public void serialize(
      Struct struct, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject();
    for (Map.Entry<String, Struct.Value> entry : struct.fields().entrySet()) {
      jsonGenerator.writeFieldName(entry.getKey());
      serializeValue(entry.getValue(), jsonGenerator, serializerProvider);
    }
    jsonGenerator.writeEndObject();
  }

  private void serializeValue(
      Value value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    Kind valueKind = value.kind();
    switch (valueKind) {
      case STRING_VALUE:
        jsonGenerator.writeString(value.stringValue());
        break;
      case BOOL_VALUE:
        jsonGenerator.writeBoolean(value.boolValue());
        break;
      case NUMBER_VALUE:
        jsonGenerator.writeNumber(value.numberValue());
        break;
      case NULL_VALUE:
        jsonGenerator.writeNull();
        break;
      case LIST_VALUE:
        jsonGenerator.writeStartArray();
        for (var subValues : value.listValue()) {
          serializeValue(subValues, jsonGenerator, serializerProvider);
        }
        jsonGenerator.writeEndArray();
        break;
      case STRUCT_VALUE:
        serialize(value.structValue(), jsonGenerator, serializerProvider);
        break;
    }
  }
}
