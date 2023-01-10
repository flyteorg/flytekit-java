package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.Serializers;

public class BindingMapSerializers extends Serializers.Base {

  @Override
  public JsonSerializer<?> findSerializer(SerializationConfig config, JavaType type,
      BeanDescription beanDesc) {
    if (type.getRawClass().equals(JacksonBindingMap.class)) {
      return new BindingMapSerializer();
    }

    return super.findSerializer(config, type, beanDesc);
  }
}
