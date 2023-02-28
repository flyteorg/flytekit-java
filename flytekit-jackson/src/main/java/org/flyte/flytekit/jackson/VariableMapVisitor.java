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

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkLiteralType;
import org.flyte.flytekit.SdkLiteralTypes;

class VariableMapVisitor extends JsonObjectFormatVisitor.Base {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER =
      Map.of(
          void.class, Void.class,
          boolean.class, Boolean.class,
          byte.class, Byte.class,
          char.class, Character.class,
          short.class, Short.class,
          int.class, Integer.class,
          long.class, Long.class,
          float.class, Float.class,
          double.class, Double.class);

  VariableMapVisitor(SerializerProvider provider) {
    super(provider);
  }

  private final Map<String, Variable> builderVariables = new LinkedHashMap<>();
  private final Map<String, AnnotatedMember> builderMembers = new LinkedHashMap<>();
  private final Map<String, SdkLiteralType<?>> builderTypes = new LinkedHashMap<>();

  @Override
  public void property(BeanProperty prop) {
    JavaType handledType = getHandledType(prop);
    String propName = prop.getName();
    AnnotatedMember member = prop.getMember();
    SdkLiteralType<?> literalType =
        toLiteralType(
            handledType,
            /*rootLevel=*/ true,
            propName,
            member.getMember().getDeclaringClass().getName());

    String description = getDescription(member);

    Variable variable =
        Variable.builder()
            .description(description)
            .literalType(literalType.getLiteralType())
            .build();

    builderMembers.put(propName, member);
    builderVariables.put(propName, variable);
    builderTypes.put(propName, literalType);
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
    return unmodifiableMap(builderVariables);
  }

  public Map<String, AnnotatedMember> getMembersMap() {
    return unmodifiableMap(builderMembers);
  }

  public Map<String, SdkLiteralType<?>> getTypesMap() {
    return unmodifiableMap(builderTypes);
  }

  private String getDescription(AnnotatedMember member) {
    var description = member.getAnnotation(Description.class);

    if (description == null) {
      return "";
    }

    return description.value();
  }

  @SuppressWarnings("AlreadyChecked")
  private SdkLiteralType<?> toLiteralType(
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
      return SdkLiteralTypes.integers();
    } else if (isPrimitiveAssignableFrom(Double.class, type)) {
      return SdkLiteralTypes.floats();
    } else if (String.class.equals(type) || javaType.isEnumType()) {
      return SdkLiteralTypes.strings();
    } else if (isPrimitiveAssignableFrom(Boolean.class, type)) {
      return SdkLiteralTypes.booleans();
    } else if (Instant.class.isAssignableFrom(type)) {
      return SdkLiteralTypes.datetimes();
    } else if (Duration.class.isAssignableFrom(type)) {
      return SdkLiteralTypes.durations();
    } else if (List.class.isAssignableFrom(type)) {
      JavaType elementType = javaType.getBindings().getBoundType(0);

      return SdkLiteralTypes.collections(
          toLiteralType(elementType, false, propName, declaringClassName));
    } else if (Map.class.isAssignableFrom(type)) {
      JavaType keyType = javaType.getBindings().getBoundType(0);
      JavaType valueType = javaType.getBindings().getBoundType(1);

      if (!keyType.getRawClass().equals(String.class)) {
        throw new UnsupportedOperationException(
            "Only Map<String, ?> is supported, got [" + javaType.getGenericSignature() + "]");
      }

      return SdkLiteralTypes.maps(toLiteralType(valueType, false, propName, declaringClassName));
    }
    // TODO: Support blobs and structs
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
