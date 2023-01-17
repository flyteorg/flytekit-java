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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Map;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.jackson.JacksonBindingMap;

public class BindingMapSerializer extends JsonSerializer<JacksonBindingMap> {
  @Override
  public void serialize(JacksonBindingMap value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    for (Map.Entry<String, SdkBindingData<?>> entry : value.getBindingsMap().entrySet()) {
      String attr = entry.getKey();
      gen.writeFieldName(attr);
      gen.writeString(attr);
    }
    gen.writeEndObject();
  }
}
