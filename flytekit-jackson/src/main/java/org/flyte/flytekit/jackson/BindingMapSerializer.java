package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.flytekit.SdkBindingData;

import java.io.IOException;
import java.util.Map;

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
