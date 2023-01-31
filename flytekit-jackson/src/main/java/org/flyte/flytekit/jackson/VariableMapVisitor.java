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
package org.flyte.flytekit.jackson;

import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.Description;
import org.flyte.flytekit.SdkBindingData;

class VariableMapVisitor extends JsonObjectFormatVisitor.Base {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER;

  VariableMapVisitor(SerializerProvider provider) {
    super(provider);
  }

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
  private final Map<String, AnnotatedMember> builderMembers = new LinkedHashMap<>();

  @Override
  public void property(BeanProperty prop) {
    JavaType handledType = getHandledType(prop);
    LiteralType literalType =
        toLiteralType(
            handledType,
            /*rootLevel=*/ true,
            prop.getName(),
            prop.getMember().getMember().getDeclaringClass().getName());

    String description =
        getDescription(
            handledType,
            prop.getMember().getMember().getDeclaringClass().getName(),
            prop.getMember());

    Variable variable =
        Variable.builder().description(description).literalType(literalType).build();

    builderMembers.put(prop.getName(), prop.getMember());
    builder.put(prop.getName(), variable);
  }

  private JavaType getHandledType(BeanProperty prop) {
    try {
      JsonSerializer<Object> serializer = getProvider().findValueSerializer(prop.getType(), prop);

      if (serializer.getDelegatee() != null) {
        // if there is a delegatee, used handled type, that is going to be
        // different from prop.getType()
        return getProvider().constructType(serializer.handledType());
      } else {
        // otherwise, always use prop.getType() because it isn't erased, e.g. has generic
        // information
        return prop.getType();
      }
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to find serializer for [%s]", prop), e);
    }
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

  public Map<String, AnnotatedMember> getMembersMap() {
    return unmodifiableMap(new HashMap<>(builderMembers));
  }

  private Method getDeclaredMethod(String name, Class<?> clazz) {
    try {
      return clazz.getDeclaredMethod(name);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(
          String.format("Couldn't find method=%s of class=%s", name, clazz.getName()));
    }
  }

  private Field getDeclaredField(String name, Class<?> clazz) {
    try {
      return clazz.getDeclaredField(name);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(
          String.format("Couldn't find field=%s of class=%s", name, clazz.getName()));
    }
  }

  private Class<?> getClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(String.format("Couldn't find class=%s", className));
    }
  }

  private String getDescription(
      JavaType javaType, String declaringClassName, AnnotatedMember member) {
    Class<?> type = javaType.getRawClass();

    if (!SdkBindingData.class.isAssignableFrom(type)) {
      return "";
    }

    String name = member.getName();
    Class<Description> descriptionAnnotationClass = Description.class;
    Class<?> clazz = getClass(declaringClassName);

    Description description;
    if (member instanceof AnnotatedMethod) {
      description =
          getDeclaredMethod(name, clazz).getDeclaredAnnotation(descriptionAnnotationClass);
    } else if (member instanceof AnnotatedField) {
      description = getDeclaredField(name, clazz).getDeclaredAnnotation(descriptionAnnotationClass);
    } else {
      throw new RuntimeException(
          String.format(
              "Member %s of class %s is not of type %s or %s for",
              name,
              declaringClassName,
              AnnotatedMethod.class.getName(),
              AnnotatedField.class.getName()));
    }

    // No description annotation present
    if (description == null) {
      return "";
    }

    return description.value();
  }

  @SuppressWarnings("AlreadyChecked")
  private LiteralType toLiteralType(
      JavaType javaType, boolean rootLevel, String propName, String declaringClassName) {
    Class<?> type = javaType.getRawClass();

    if (SdkBindingData.class.isAssignableFrom(type)) {
      return toLiteralType(
          javaType.getBindings().getBoundType(0), false, propName, declaringClassName);
    } else if (rootLevel) {
      throw new UnsupportedOperationException(
          String.format(
              "Field '%s' from class '%s' is declared as '%s' and it is not matching any of the supported types. "
                  + "Please make sure your variable declared type is wrapped in 'SdkBindingData<>'.",
              propName, declaringClassName, type));
    } else if (isPrimitiveAssignableFrom(Long.class, type)) {
      return LiteralTypes.INTEGER;
    } else if (isPrimitiveAssignableFrom(Double.class, type)) {
      return LiteralTypes.FLOAT;
    } else if (String.class.equals(type) || javaType.isEnumType()) {
      return LiteralTypes.STRING;
    } else if (isPrimitiveAssignableFrom(Boolean.class, type)) {
      return LiteralTypes.BOOLEAN;
    } else if (Instant.class.isAssignableFrom(type)) {
      return LiteralTypes.DATETIME;
    } else if (Duration.class.isAssignableFrom(type)) {
      return LiteralTypes.DURATION;
    } else if (List.class.isAssignableFrom(type)) {
      JavaType elementType = javaType.getBindings().getBoundType(0);

      return LiteralType.ofCollectionType(
          toLiteralType(elementType, false, propName, declaringClassName));
    } else if (Map.class.isAssignableFrom(type)) {
      JavaType keyType = javaType.getBindings().getBoundType(0);
      JavaType valueType = javaType.getBindings().getBoundType(1);

      if (!keyType.getRawClass().equals(String.class)) {
        throw new UnsupportedOperationException(
            "Only Map<String, ?> is supported, got [" + javaType.getGenericSignature() + "]");
      }

      return LiteralType.ofMapValueType(
          toLiteralType(valueType, false, propName, declaringClassName));
    } else if (Blob.class.isAssignableFrom(type)) {
      // TODO add annotation to specify dimensionality and format
      BlobType blobType =
          BlobType.builder().format("").dimensionality(BlobType.BlobDimensionality.SINGLE).build();

      return LiteralType.ofBlobType(blobType);
    }

    BeanDescription bean = getProvider().getConfig().introspect(javaType);
    List<BeanPropertyDefinition> properties = bean.findProperties();

    if (properties.isEmpty()) {
      // doesn't look like a bean, can be java.lang.Integer, or something else
      throw new UnsupportedOperationException(
          String.format("Unsupported type: [%s]", type.getName()));
    } else {
      return LiteralType.ofSimpleType(SimpleType.STRUCT);
    }
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
