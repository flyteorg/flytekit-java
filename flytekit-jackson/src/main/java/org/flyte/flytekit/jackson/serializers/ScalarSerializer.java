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

import static org.flyte.flytekit.jackson.util.JacksonConstants.SCALAR;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

public abstract class ScalarSerializer extends LiteralSerializer {

  public ScalarSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    super(gen, key, value, serializerProvider, literalType);
  }

  @Override
  final void serializeLiteral() throws IOException {
    gen.writeObject(Literal.Kind.SCALAR);
    gen.writeFieldName(SCALAR);
    serializeScalar();
  }

  abstract void serializeScalar() throws IOException;
}
