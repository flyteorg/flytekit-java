package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.Serializers;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

public class LiteralMapSerializers extends Serializers.Base {

  @Override
  public JsonSerializer<?> findSerializer(SerializationConfig config, JavaType type,
      BeanDescription beanDesc) {
    if (type.getRawClass().equals(JacksonLiteralMap.class)) {
      return new LiteralMapSerializer();
    }

    return super.findSerializer(config, type, beanDesc);
  }
}
