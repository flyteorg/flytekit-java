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
package org.flyte.flytekit;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.FormatMethod;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;

/**
 * Mapping between {@link AutoValue} classes and Flyte {@link TypedInterface} and {@link Literal}.
 */
class AutoValueReflection {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER;

  private AutoValueReflection() {
    throw new UnsupportedOperationException();
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

  static Map<String, Variable> interfaceOf(Class<?> cls) {
    if (Void.class.equals(cls)) {
      return Collections.emptyMap();
    }

    // there are 3 ways to create auto-value object:

    // 1. create method, if it has it
    // TODO

    // 2. builder, if it has it
    // TODO

    // 3. generated constructor
    return interfaceOfGeneratedConstructor(cls);
  }

  static <T> T readValue(Map<String, Literal> inputs, Class<T> cls) {
    Map<String, Object> inputValues = toJavaMap(inputs);
    Constructor<T> constructor = getAutoValueConstructor(cls);
    Object[] paramValues =
        Arrays.stream(constructor.getParameters())
            .map(param -> getParamValue(inputValues, param))
            .toArray();
    try {
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }

      return constructor.newInstance(paramValues);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Couldn't instantiate class [%s] with values [%s]",
              constructor.getDeclaringClass(), inputs),
          e);
    }
  }

  static <T> Map<String, Literal> toLiteralMap(T object, Class<T> cls) {
    return interfaceOf(cls).entrySet().stream()
        .map(
            entry -> {
              Object value = getVariableValue(entry.getKey(), object, cls);
              Literal literal =
                  value != null ? toLiteral(value, entry.getValue().literalType()) : null;
              return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), literal);
            })
        .filter(entry -> entry.getValue() != null)
        .collect(
            collectingAndThen(
                toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap));
  }

  private static Object getParamValue(Map<String, Object> inputValues, Parameter param) {
    String paramName = param.getName();
    checkArgument(
        inputValues.containsKey(paramName),
        "Constructor param [%s] is not in inputs [%s]",
        paramName,
        inputValues);

    Object value = inputValues.get(paramName);
    checkArgument(
        isPrimitiveAssignableFrom(value.getClass(), param.getType()),
        "Constructor param [%s] is not assignable from [%s]",
        paramName,
        value);

    return value;
  }

  private static <T> Object getVariableValue(String name, T object, Class<T> cls) {
    try {
      Method method = cls.getDeclaredMethod(name);
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(object);
    } catch (NoSuchMethodException
        | IllegalArgumentException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new IllegalArgumentException(
          String.format("Can't find or invoke method [%s()] on [%s]", name, cls.getName()), e);
    }
  }

  private static boolean isPrimitiveAssignableFrom(Class<?> fromClass, Class<?> toClass) {
    return tryWrap(toClass).isAssignableFrom(tryWrap(fromClass));
  }

  private static Map<String, Variable> interfaceOfGeneratedConstructor(Class<?> cls) {
    Constructor<?> constructor = getAutoValueConstructor(cls);
    return interfaceOfConstructor(constructor);
  }

  private static Map<String, Variable> interfaceOfConstructor(Constructor<?> constructor) {
    return Stream.of(constructor.getParameters())
        .sorted(Comparator.comparing(Parameter::getName))
        .map(AutoValueReflection::toNameVariable)
        .collect(
            collectingAndThen(
                toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap));
  }

  @SuppressWarnings("unchecked")
  private static <T> Constructor<T> getAutoValueConstructor(Class<T> cls) {
    Class<T> generatedClass = getGeneratedClass(cls);

    return Arrays.stream((Constructor<T>[]) generatedClass.getDeclaredConstructors())
        .filter(c -> !Modifier.isPrivate(c.getModifiers()))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Can't find constructor on [%s]", generatedClass.getName())));
  }

  private static Map.Entry<String, Variable> toNameVariable(Parameter parameter) {
    checkArgument(
        parameter.isNamePresent(),
        "Java bytecode doesn't contain parameter name information, to resolve either "
            + "add \"-parameters\" flag to javac or declare create method in AutoValue class");

    String description = ""; // TODO add annotation for description

    return new SimpleImmutableEntry<>(
        parameter.getName(),
        Variable.builder()
            .literalType(toLiteralType(parameter.getType()))
            .description(description)
            .build());
  }

  private static LiteralType toLiteralType(Class<?> type) {
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
      // TODO: Support Literal Collections of any LiteralType. Now we support only strings
      return LiteralType.ofCollectionType(LiteralTypes.STRING);
    } else if (Map.class.isAssignableFrom(type)) {
      // TODO: Support Literal Maps of any LiteralType. Now we support only strings for values
      return LiteralType.ofMapValueType(LiteralTypes.STRING);
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported type: [%s]", type.getName()));
  }

  @SuppressWarnings("unchecked")
  private static Literal toLiteral(Object value, LiteralType literalType) {
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        SimpleType simpleType = literalType.simpleType();
        return toLiteral(value, simpleType);
      case COLLECTION_TYPE:
        List<?> list = (List) value;
        LiteralType elementType = literalType.collectionType();
        return Literal.ofCollection(
            list.stream()
                .map(element -> toLiteral(element, elementType))
                .collect(collectingAndThen(toList(), Collections::unmodifiableList)));
      case MAP_VALUE_TYPE:
        Map<String, ?> map = (Map<String, ?>) value;
        LiteralType valuesType = literalType.mapValueType();
        return Literal.ofMap(
            map.entrySet().stream()
                .map(
                    entry ->
                        new SimpleImmutableEntry<>(
                            entry.getKey(), toLiteral(entry.getValue(), valuesType)))
                .collect(
                    collectingAndThen(
                        toMap(Map.Entry::getKey, Map.Entry::getValue),
                        Collections::unmodifiableMap)));
      case SCHEMA_TYPE:
        // TODO: Add support for Schema Types
      case BLOB_TYPE:
        // TODO: Add support for Blob Types
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported literal type: [%s]", literalType));
  }

  private static Literal toLiteral(Object value, SimpleType simpleType) {
    switch (simpleType) {
      case INTEGER:
        return toLiteral(Primitive.ofInteger((Long) value));
      case FLOAT:
        return toLiteral(Primitive.ofFloat((Double) value));
      case STRING:
        return toLiteral(Primitive.ofString((String) value));
      case BOOLEAN:
        return toLiteral(Primitive.ofBoolean((Boolean) value));
      case DATETIME:
        return toLiteral(Primitive.ofDatetime((Instant) value));
      case DURATION:
        return toLiteral(Primitive.ofDuration((Duration) value));
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported simple type: [%s]", simpleType));
  }

  private static Literal toLiteral(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getGeneratedClass(Class<T> clazz) {
    String generatedClassName = getAutoValueGeneratedName(clazz.getName());

    Class<?> generatedClass;
    try {
      generatedClass = Class.forName(generatedClassName, true, clazz.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("AutoValue generated class not found for [%s]", clazz), e);
    }
    checkArgument(
        clazz.isAssignableFrom(generatedClass),
        "Generated class [%s] is not assignable to [%s]",
        generatedClass,
        clazz);
    return (Class<T>) generatedClass;
  }

  private static String getAutoValueGeneratedName(String baseClass) {
    int lastDot = baseClass.lastIndexOf('.');
    String packageName = baseClass.substring(0, lastDot);
    String baseName = baseClass.substring(lastDot + 1).replace('$', '_');

    return packageName + ".AutoValue_" + baseName;
  }

  private static Map<String, Object> toJavaMap(Map<String, Literal> literalMap) {
    Map<String, Object> javaMap = new HashMap<>();
    literalMap.forEach((key, value) -> javaMap.put(key, toJavaObject(value)));
    return javaMap;
  }

  private static Object toJavaObject(Literal literal) {
    switch (literal.kind()) {
      case SCALAR:
        return toJavaObject(literal.scalar());
      case COLLECTION:
        return literal.collection().stream()
            .map(AutoValueReflection::toJavaObject)
            .collect(toList());
      case MAP:
        return literal.map().entrySet().stream()
            .map(
                entry -> new SimpleImmutableEntry<>(entry.getKey(), toJavaObject(entry.getValue())))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
  }

  private static Object toJavaObject(Scalar scalar) {
    if (scalar.primitive() != null) {
      return toJavaObject(scalar.primitive());
    }

    throw new UnsupportedOperationException(String.format("Unsupported Scalar [%s]", scalar));
  }

  private static Object toJavaObject(Primitive primitive) {
    switch (primitive.type()) {
      case INTEGER:
        return primitive.integer();
      case FLOAT:
        return primitive.float_();
      case STRING:
        return primitive.string();
      case BOOLEAN:
        return primitive.boolean_();
      case DATETIME:
        return primitive.datetime();
      case DURATION:
        return primitive.duration();
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }

  @FormatMethod
  private static void checkArgument(
      boolean booleanValue, String errorMessageTemplate, Object... errorMessageArgs) {
    if (!booleanValue) {
      throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
    }
  }

  private static <T> Class<T> tryWrap(Class<T> cls) {
    @SuppressWarnings("unchecked")
    Class<T> wrapper = (Class<T>) PRIMITIVE_TO_WRAPPER.get(cls);
    return wrapper != null ? wrapper : cls;
  }
}
