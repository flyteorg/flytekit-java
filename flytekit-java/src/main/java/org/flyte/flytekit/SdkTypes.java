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
package org.flyte.flytekit;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

/** An utility class for creating {@link SdkType} objects for different types. */
public class SdkTypes {
  private SdkTypes() {}

  public static SdkType<Void> nulls() {
    return new VoidSdkType();
  }

  private static class VoidSdkType extends SdkType<Void> {

    @Override
    public Map<String, Literal> toLiteralMap(Void value) {
      return Collections.emptyMap();
    }

    @Override
    public Void fromLiteralMap(Map<String, Literal> value) {
      return null;
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Collections.emptyMap();
    }
  }

  // private custom class instead of autovalue to hide everything
  private static class TypeToLiteralTypeDef<T> {
    final Function<T, Literal> toLiteral;
    final Function<Literal, T> toValue;
    final LiteralType type;

    private TypeToLiteralTypeDef(
        Function<T, Literal> toLiteral, Function<Literal, T> toValue, LiteralType type) {
      this.toLiteral = toLiteral;
      this.toValue = toValue;
      this.type = type;
    }
  }

  private static final Map<Class<?>, TypeToLiteralTypeDef<?>> TYPE_MAP;

  static {
    Map<Class<?>, TypeToLiteralTypeDef<?>> typeMap = new HashMap<>();
    typeMap.put(
        Long.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofInteger, l -> l.scalar().primitive().integerValue(), LiteralTypes.INTEGER));
    typeMap.put(
        Double.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofFloat, l -> l.scalar().primitive().floatValue(), LiteralTypes.FLOAT));
    typeMap.put(
        String.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofString, l -> l.scalar().primitive().stringValue(), LiteralTypes.STRING));
    typeMap.put(
        Boolean.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofBoolean, l -> l.scalar().primitive().booleanValue(), LiteralTypes.BOOLEAN));
    typeMap.put(
        Instant.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofDatetime, l -> l.scalar().primitive().datetime(), LiteralTypes.DATETIME));
    typeMap.put(
        Duration.class,
        new TypeToLiteralTypeDef<>(
            Literals::ofDuration, l -> l.scalar().primitive().duration(), LiteralTypes.DURATION));

    TYPE_MAP = typeMap;
  }

  public static <T> SdkType<T> ofPrimitive(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkLiteralType<>(requireNonNull(varName), typeToLiteralTypeDef);
  }

  public static <T> SdkType<List<T>> ofCollection(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkCollectionType<>(varName, typeToLiteralTypeDef);
  }

  public static <T> SdkType<Map<String, T>> ofMap(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkMapType<>(varName, typeToLiteralTypeDef);
  }

  private static <T> TypeToLiteralTypeDef<T> getDef(Class<T> clazz) {
    if (!TYPE_MAP.containsKey(clazz)) {
      String errMessage = String.format("Type [%s] is not a supported literal type", clazz);
      throw new IllegalArgumentException(errMessage);
    }
    @SuppressWarnings("unchecked")
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = (TypeToLiteralTypeDef<T>) TYPE_MAP.get(clazz);
    return typeToLiteralTypeDef;
  }

  private static class SdkLiteralType<T> extends SdkType<T> {
    private final String varName;
    private final TypeToLiteralTypeDef<T> typeDef;

    private SdkLiteralType(String varName, TypeToLiteralTypeDef<T> typeDef) {
      this.varName = varName;
      this.typeDef = typeDef;
    }

    @Override
    public Map<String, Literal> toLiteralMap(T value) {
      return singletonMap(varName, typeDef.toLiteral.apply(value));
    }

    @Override
    public T fromLiteralMap(Map<String, Literal> map) {
      return typeDef.toValue.apply(map.get(varName));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return singletonMap(varName, Variable.builder().literalType(typeDef.type).build());
    }
  }

  private static class SdkCollectionType<T> extends SdkType<List<T>> {
    private final String varName;
    private final TypeToLiteralTypeDef<T> typeDef;

    private SdkCollectionType(String varName, TypeToLiteralTypeDef<T> typeDef) {
      this.varName = varName;
      this.typeDef = typeDef;
    }

    @Override
    public Map<String, Literal> toLiteralMap(List<T> values) {

      List<Literal> collection = values.stream().map(typeDef.toLiteral).collect(toList());
      return singletonMap(varName, Literal.ofCollection(collection));
    }

    @Override
    public List<T> fromLiteralMap(Map<String, Literal> map) {
      Literal collection = map.get(varName);
      return collection.collection().stream().map(typeDef.toValue).collect(toList());
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return singletonMap(
          varName,
          Variable.builder().literalType(LiteralType.ofCollectionType(typeDef.type)).build());
    }
  }

  private static class SdkMapType<T> extends SdkType<Map<String, T>> {
    private final String varName;
    private final TypeToLiteralTypeDef<T> typeDef;

    private SdkMapType(String varName, TypeToLiteralTypeDef<T> typeDef) {
      this.varName = varName;
      this.typeDef = typeDef;
    }

    @Override
    public Map<String, Literal> toLiteralMap(Map<String, T> values) {
      Map<String, Literal> map =
          values.entrySet().stream()
              .map(
                  e ->
                      new AbstractMap.SimpleEntry<>(
                          e.getKey(), typeDef.toLiteral.apply(e.getValue())))
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
      return singletonMap(varName, Literal.ofMap(map));
    }

    @Override
    public Map<String, T> fromLiteralMap(Map<String, Literal> map) {
      Literal collection = map.get(varName);
      return collection.map().entrySet().stream()
          .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), typeDef.toValue.apply(e.getValue())))
          .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return singletonMap(
          varName,
          Variable.builder().literalType(LiteralType.ofMapValueType(typeDef.type)).build());
    }
  }
}
