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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Primitives;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Timestamp;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;

/**
 * Mapping between {@link AutoValue} classes and Flyte {@link TypedInterface} and {@link Literal}.
 */
public class AutoValueReflection {

  public static Map<String, Variable> interfaceOf(TypeDescriptor<?> cls) {
    return interfaceOf(cls.getRawType());
  }

  public static Map<String, Variable> interfaceOf(Class<?> cls) {
    if (Void.class.equals(cls)) {
      return ImmutableMap.of();
    }

    // there are 3 ways to create auto-value object:

    // 1. create method, if it has it
    // TODO

    // 2. builder, if it has it
    // TODO

    // 3. generated constructor
    return interfaceOfGeneratedConstructor(cls);
  }

  public static <T> T readValue(Map<String, Literal> inputs, Class<T> cls) {
    Map<String, Object> inputValues = toJavaMap(inputs);
    Constructor<T> constructor = getAutoValueConstructor(cls);
    Object[] paramValues =
        Arrays.stream(constructor.getParameters())
            .map(param -> getParamValue(inputValues, param))
            .toArray();
    try {
      return constructor.newInstance(paramValues);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Couldn't instantiate class [%s] with values [%s]",
              constructor.getDeclaringClass(), inputs),
          e);
    }
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

  private static boolean isPrimitiveAssignableFrom(Class<?> fromClass, Class<?> toClass) {
    return Primitives.wrap(toClass).isAssignableFrom(Primitives.wrap(fromClass));
  }

  private static Map<String, Variable> interfaceOfGeneratedConstructor(Class<?> cls) {
    Constructor<?> constructor = getAutoValueConstructor(cls);
    return interfaceOfConstructor(constructor);
  }

  private static Map<String, Variable> interfaceOfConstructor(Constructor<?> constructor) {
    return Stream.of(constructor.getParameters())
        .sorted(Comparator.comparing(Parameter::getName))
        .map(AutoValueReflection::toNameVariable)
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
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

    return Maps.immutableEntry(
        parameter.getName(), Variable.create(toLiteralType(parameter.getType()), description));
  }

  private static LiteralType toLiteralType(Class<?> type) {
    if (isPrimitiveAssignableFrom(Long.class, type)) {
      return LiteralType.create(SimpleType.INTEGER);
    } else if (isPrimitiveAssignableFrom(Double.class, type)) {
      return LiteralType.create(SimpleType.FLOAT);
    } else if (String.class.isAssignableFrom(type)) {
      return LiteralType.create(SimpleType.STRING);
    } else if (isPrimitiveAssignableFrom(Boolean.class, type)) {
      return LiteralType.create(SimpleType.BOOLEAN);
    } else if (Timestamp.class.isAssignableFrom(type)) {
      return LiteralType.create(SimpleType.DATETIME);
    } else if (Duration.class.isAssignableFrom(type)) {
      return LiteralType.create(SimpleType.DURATION);
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported type: [%s]", type.getName()));
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<T> getGeneratedClass(Class<T> clazz) {
    String generatedClassName = getAutoValueGeneratedName(clazz.getName());

    Class<?> generatedClass;
    try {
      generatedClass = Class.forName(generatedClassName);
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

    for (Map.Entry<String, Literal> entry : literalMap.entrySet()) {
      Literal literal = entry.getValue();

      if (literal.scalar() != null) {
        javaMap.put(entry.getKey(), toJavaObject(literal.scalar()));
      } else {
        throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
      }
    }

    return javaMap;
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
}
