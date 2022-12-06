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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Variable;

/** A utility class for creating {@link SdkType} objects for different types. */
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
    final BiFunction<String, T, Literal> toLiteral;
    final BiFunction<String, Literal, T> toValue;
    final LiteralType type;

    private TypeToLiteralTypeDef(
        BiFunction<String, T, Literal> toLiteral,
        BiFunction<String, Literal, T> toValue,
        LiteralType type) {
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
            (n, v) -> Literals.ofInteger(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.INTEGER_VALUE, Primitive::integerValue),
            LiteralTypes.INTEGER));
    typeMap.put(
        Double.class,
        new TypeToLiteralTypeDef<>(
            (n, v) -> Literals.ofFloat(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.FLOAT_VALUE, Primitive::floatValue),
            LiteralTypes.FLOAT));
    typeMap.put(
        String.class,
        new TypeToLiteralTypeDef<>(
            (n, v) -> Literals.ofString(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.STRING_VALUE, Primitive::stringValue),
            LiteralTypes.STRING));
    typeMap.put(
        Boolean.class,
        new TypeToLiteralTypeDef<>(
            (n, v) -> Literals.ofBoolean(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.BOOLEAN_VALUE, Primitive::booleanValue),
            LiteralTypes.BOOLEAN));
    typeMap.put(
        Instant.class,
        new TypeToLiteralTypeDef<>(
            (n, v) -> Literals.ofDatetime(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.DATETIME, Primitive::datetime),
            LiteralTypes.DATETIME));
    typeMap.put(
        Duration.class,
        new TypeToLiteralTypeDef<>(
            (n, v) -> Literals.ofDuration(v),
            (n, l) -> getPrimitive(n, l, Primitive.Kind.DURATION, Primitive::duration),
            LiteralTypes.DURATION));

    TYPE_MAP = typeMap;
  }

  private static <T> T getPrimitive(
      String name, Literal l, Primitive.Kind kind, Function<Primitive, T> function) {
    if (l.kind() != Literal.Kind.SCALAR
        || l.scalar().kind() != Scalar.Kind.PRIMITIVE
        || l.scalar().primitive().kind() != kind) {
      throw new IllegalArgumentException(
          String.format("Type missmatch, expected %s to be a primitive %s but got: %s", name, kind, l));
    }
    return function.apply(l.scalar().primitive());
  }

  public static <T> SdkType<T> ofPrimitive(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkLiteralType<>(ensureValidVarName(varName), typeToLiteralTypeDef);
  }

  public static <T> SdkType<List<T>> ofCollection(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkCollectionType<>(ensureValidVarName(varName), typeToLiteralTypeDef);
  }

  public static <T> SdkType<Map<String, T>> ofMap(String varName, Class<T> clazz) {
    TypeToLiteralTypeDef<T> typeToLiteralTypeDef = getDef(clazz);
    return new SdkMapType<>(ensureValidVarName(varName), typeToLiteralTypeDef);
  }

  private static String ensureValidVarName(String varName) {
    if (varName == null || varName.trim().isEmpty()) {
      throw new IllegalArgumentException("Variable name shouldn't be null or empty");
    }
    return varName;
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

  private static void checkVariableName(String varName, Map<String, Literal> map) {
    if (!map.containsKey(varName)) {
      String errMsg =
          String.format(
              "Variable name [%s] missing among the the names in literal map: %s",
              varName, map.keySet());
      throw new IllegalArgumentException(errMsg);
    }
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
      return singletonMap(varName, typeDef.toLiteral.apply(varName, value));
    }

    @Override
    public T fromLiteralMap(Map<String, Literal> map) {
      checkVariableName(varName, map);
      return typeDef.toValue.apply(varName, map.get(varName));
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

      List<Literal> collection =
          values.stream().map(v -> typeDef.toLiteral.apply(varName, v)).collect(toList());
      return singletonMap(varName, Literal.ofCollection(collection));
    }

    @Override
    public List<T> fromLiteralMap(Map<String, Literal> map) {
      checkVariableName(varName, map);
      Literal collection = map.get(varName);
      if (collection.kind() != Literal.Kind.COLLECTION) {
        String errMsg =
            String.format(
                "Type missmatch, expecting [%s] to be a collection but is [%s] instead",
                varName, collection.kind());
        throw new IllegalArgumentException(errMsg);
      }
      return collection.collection().stream()
          .map(v -> typeDef.toValue.apply(varName, v))
          .collect(toList());
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
                  e -> {
                    String name = e.getKey();
                    return new MapEntry<>(name, typeDef.toLiteral.apply(name, e.getValue()));
                  })
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
      return singletonMap(varName, Literal.ofMap(map));
    }

    @Override
    public Map<String, T> fromLiteralMap(Map<String, Literal> map) {
      checkVariableName(varName, map);
      Literal internalMap = map.get(varName);
      if (internalMap.kind() != Literal.Kind.MAP) {
        String errMsg =
            String.format(
                "Type missmatch, expecting [%s] to be a map but is [%s] instead",
                varName, internalMap.kind());
        throw new IllegalArgumentException(errMsg);
      }

      return internalMap.map().entrySet().stream()
          .map(
              e -> {
                String name = e.getKey();
                return new MapEntry<>(name, typeDef.toValue.apply(name, e.getValue()));
              })
          .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return singletonMap(
          varName,
          Variable.builder().literalType(LiteralType.ofMapValueType(typeDef.type)).build());
    }
  }

  //TODO: Remove this class when compiling with Java 11 and we can use Map.entry()
  // using new AbstractMap.SimpleEntry directly is too verbose
  private static class MapEntry<T> extends AbstractMap.SimpleEntry<String, T> {

    private MapEntry(String key, T value) {
      super(key, value);
    }
  }
}
