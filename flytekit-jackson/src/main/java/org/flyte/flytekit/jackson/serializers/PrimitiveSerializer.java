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
package org.flyte.flytekit.jackson.serializers;

import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.PRIMITIVE;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.VALUE;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

public abstract class PrimitiveSerializer extends ScalarSerializer {

  public PrimitiveSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    super(gen, key, value, serializerProvider, literalType);
  }

  @Override
  public final void serializeScalar() throws IOException {
    gen.writeObject(Scalar.Kind.PRIMITIVE);
    serializePrimitive();
  }

  abstract void serializePrimitive() throws IOException;

  protected void writePrimitive(Object kind, WritePrimitiveFunction writeValueFunction)
      throws IOException {
    gen.writeFieldName(PRIMITIVE);
    gen.writeObject(kind);
    gen.writeFieldName(VALUE);
    writeValueFunction.write(gen, value.scalar().primitive());
  }

  interface WritePrimitiveFunction {
    void write(JsonGenerator gen, Primitive value) throws IOException;
  }
}
