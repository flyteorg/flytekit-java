/*
 * Copyright 2020-2023 Flyte Authors.
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
import java.io.IOException;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.SimpleType;

public class BooleanSerializer extends PrimitiveSerializer {
  public BooleanSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    super(gen, key, value, serializerProvider, literalType);
    if (literalType.getKind() != LiteralType.Kind.SIMPLE_TYPE
        && literalType.simpleType() != SimpleType.BOOLEAN) {
      throw new IllegalArgumentException("Literal type should be a boolean literal type");
    }
  }

  @Override
  public void serializePrimitive() throws IOException {
    writePrimitive(
        Primitive.Kind.BOOLEAN_VALUE,
        (gen, primitive) -> gen.writeBoolean(primitive.booleanValue()));
  }
}
