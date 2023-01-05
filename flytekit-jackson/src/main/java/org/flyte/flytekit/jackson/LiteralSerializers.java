package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.Serializers;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

public class LiteralSerializers extends Serializers.Base {

  @Override
  public JsonSerializer<?> findSerializer(SerializationConfig config, JavaType type,
      BeanDescription beanDesc) {
    //TODO: We need to transform this into JacksonLiteralMap to have knowledge about the field names.
    if (Literal.class.isAssignableFrom(type.getRawClass())) {
      Map<String, LiteralType> literalTypeMap = type.getValueHandler();

      return new LiteralSerializer(literalTypeMap);
    }


    return super.findSerializer(config, type, beanDesc);
  }
}
