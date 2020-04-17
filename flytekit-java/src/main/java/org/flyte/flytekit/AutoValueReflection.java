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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
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
public class AutoValueReflection {
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

  @SuppressWarnings("unchecked")
  public static <T> T readValue(Map<String, Literal> inputs, Class<? extends T> cls) {
    // TODO we can get rid of jackson, given that we already do a lot of reflection

    return (T) MAPPER.convertValue(toJavaMap(inputs), AutoValueReflection.getGeneratedClass(cls));
  }

  private static Map<String, Variable> interfaceOfGeneratedConstructor(Class<?> cls) {
    Class<?> generatedClass = getGeneratedClass(cls);

    Constructor<?> constructor =
        Arrays.stream(generatedClass.getDeclaredConstructors())
            .filter(c -> !Modifier.isPrivate(c.getModifiers()))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Can't find constructor on [%s]", cls.getName())));

    return Stream.of(constructor.getParameters())
        .sorted(Comparator.comparing(Parameter::getName))
        .map(AutoValueReflection::toNameVariable)
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Map.Entry<String, Variable> toNameVariable(Parameter parameter) {
    Preconditions.checkArgument(
        parameter.isNamePresent(),
        "Java bytecode doesn't contain parameter name information, to resolve either "
            + "add \"-parameters\" flag to javac or declare create method in AutoValue class");

    String description = ""; // TODO add annotation for description

    return Maps.immutableEntry(
        parameter.getName(), Variable.create(toLiteralType(parameter.getType()), description));
  }

  private static LiteralType toLiteralType(Class<?> type) {
    if (String.class.isAssignableFrom(type)) {
      return LiteralType.create(SimpleType.STRING);
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported type: [%s]", type.getName()));
  }

  public static Class<?> getGeneratedClass(Class<?> clazz) {
    String generatedClassName = getAutoValueGeneratedName(clazz.getName());

    try {
      return Class.forName(generatedClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          String.format("AutoValue generated class not found for [%s]", generatedClassName), e);
    }
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
    if (primitive.string() != null) {
      return primitive.string();
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }
}
