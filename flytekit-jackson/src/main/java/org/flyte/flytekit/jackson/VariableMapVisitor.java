/*
 * Copyright 2020 Spotify AB.
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
package org.flyte.flytekit.jackson;

import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

class VariableMapVisitor extends JsonObjectFormatVisitor.Base {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER;

  static {
    Map<Class<?>, Class<?>> map = new HashMap<>();
    map.put(void.class, Void.class);
    map.put(boolean.class, Boolean.class);
    map.put(byte.class, Byte.class);
    map.put(char.class, Character.class);
    map.put(short.class, Short.class);
    map.put(int.class, Integer.class);
    map.put(long.class, Long.class);
    map.put(float.class, Float.class);
    map.put(double.class, Double.class);
    PRIMITIVE_TO_WRAPPER = unmodifiableMap(map);
  }

  private final Map<String, Variable> builder = new LinkedHashMap<>();

  @Override
  public void property(BeanProperty prop) {
    LiteralType literalType = toLiteralType(prop.getType());
    Variable variable = Variable.builder().description("").literalType(literalType).build();

    builder.put(prop.getName(), variable);
  }

  @Override
  public void optionalProperty(BeanProperty prop) {
    // by default all properties are optional, but flyteidl doesn't support optional
    // properties, so we always threat every property as required
    property(prop);
  }

  public Map<String, Variable> getVariableMap() {
    return unmodifiableMap(new HashMap<>(builder));
  }

  private static LiteralType toLiteralType(JavaType javaType) {
    Class<?> type = javaType.getRawClass();

    if (isPrimitiveAssignableFrom(Long.class, type)) {
      return LiteralTypes.INTEGER;
    } else if (isPrimitiveAssignableFrom(Double.class, type)) {
      return LiteralTypes.FLOAT;
    } else if (String.class == type) {
      return LiteralTypes.STRING;
    } else if (isPrimitiveAssignableFrom(Boolean.class, type)) {
      return LiteralTypes.BOOLEAN;
    } else if (Instant.class.isAssignableFrom(type)) {
      return LiteralTypes.DATETIME;
    } else if (Duration.class.isAssignableFrom(type)) {
      return LiteralTypes.DURATION;
    } else if (List.class.isAssignableFrom(type)) {
      JavaType elementType = javaType.getBindings().getBoundType(0);

      return LiteralType.ofCollectionType(toLiteralType(elementType));
    } else if (Map.class.isAssignableFrom(type)) {
      JavaType keyType = javaType.getBindings().getBoundType(0);
      JavaType valueType = javaType.getBindings().getBoundType(1);

      if (!keyType.getRawClass().equals(String.class)) {
        throw new UnsupportedOperationException(
            "Only Map<String, ?> is supported, got [" + javaType.getGenericSignature() + "]");
      }

      return LiteralType.ofMapValueType(toLiteralType(valueType));
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported type: [%s]", type.getName()));
  }

  private static boolean isPrimitiveAssignableFrom(Class<?> fromClass, Class<?> toClass) {
    return tryWrap(toClass).isAssignableFrom(tryWrap(fromClass));
  }

  private static <T> Class<T> tryWrap(Class<T> cls) {
    @SuppressWarnings("unchecked")
    Class<T> wrapper = (Class<T>) PRIMITIVE_TO_WRAPPER.get(cls);
    return wrapper != null ? wrapper : cls;
  }
}
