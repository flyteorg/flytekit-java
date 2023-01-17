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

import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.LITERAL;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

abstract class LiteralSerializer {

  protected final JsonGenerator gen;
  protected final String key;
  protected final Literal value;
  protected final SerializerProvider serializerProvider;
  protected final LiteralType literalType;

  public LiteralSerializer(
      JsonGenerator gen,
      String key,
      Literal value,
      SerializerProvider serializerProvider,
      LiteralType literalType) {
    this.gen = gen;
    this.key = key;
    this.value = value;
    this.serializerProvider = serializerProvider;
    this.literalType = literalType;
  }

  final void serialize() throws IOException {
    gen.writeFieldName(LITERAL);
    serializeLiteral();
  }

  abstract void serializeLiteral() throws IOException;
}
