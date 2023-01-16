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
import org.flyte.api.v1.Literal;

class LiteralMapSerializer extends StdSerializer<JacksonLiteralMap> {
  private static final long serialVersionUID = 0L;

  public LiteralMapSerializer() {
    super(JacksonLiteralMap.class);
  }

  @Override
  public void serialize(JacksonLiteralMap map, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    map.getLiteralMap().forEach((k, v) -> writeLiteralMapEntry(map, gen, serializers, k, v));
    gen.writeEndObject();
  }

  private static void writeLiteralMapEntry(
      JacksonLiteralMap map,
      JsonGenerator gen,
      SerializerProvider serializers,
      String k,
      Literal v) {
    try {
      gen.writeFieldName(k);
      gen.writeStartObject();
      LiteralSerializer literalSerializer =
          LiteralSerializerFactory.create(k, v, gen, serializers, map.getLiteralTypeMap().get(k));
      literalSerializer.serialize();
      gen.writeEndObject();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
