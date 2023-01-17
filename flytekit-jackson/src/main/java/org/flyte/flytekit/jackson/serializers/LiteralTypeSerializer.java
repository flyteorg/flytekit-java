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

import static org.flyte.flytekit.jackson.util.JacksonConstants.KIND;
import static org.flyte.flytekit.jackson.util.JacksonConstants.TYPE;
import static org.flyte.flytekit.jackson.util.JacksonConstants.VALUE;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import org.flyte.api.v1.LiteralType;

class LiteralTypeSerializer {
  static void serialize(LiteralType literalType, JsonGenerator gen) throws IOException {
    gen.writeFieldName(TYPE);

    gen.writeStartObject();
    gen.writeFieldName(KIND);
    gen.writeObject(literalType.getKind());
    gen.writeFieldName(VALUE);
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        // {type: {kind: simple, value: string}}
        gen.writeObject(literalType.simpleType());
        break;
      case COLLECTION_TYPE:
        // {type: {kind: collection, value: {type:{{kind: simple, value: string}}}}}}
        gen.writeStartObject();
        serialize(literalType.collectionType(), gen);
        gen.writeEndObject();
        break;
      case MAP_VALUE_TYPE:
        // {type: {kind: map, value: {type:{{kind: simple, value: string}}}}}}
        gen.writeStartObject();
        serialize(literalType.mapValueType(), gen);
        gen.writeEndObject();
        break;
      case SCHEMA_TYPE:
      case BLOB_TYPE:
        throw new IllegalArgumentException(
            String.format("Unsupported LiteralType.Kind: [%s]", literalType.getKind()));
    }
    gen.writeEndObject();
  }
}
