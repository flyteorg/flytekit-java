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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.flyte.flytekit.jackson.ObjectMapperUtils.createObjectMapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkLiteralType;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.jackson.deserializers.CustomSdkBindingDataDeserializers;
import org.flyte.flytekit.jackson.deserializers.LiteralMapDeserializer;

/**
 * Implementation of {@link org.flyte.flytekit.SdkType} for {@link com.google.auto.value.AutoValue}s
 * and other java types with Jackson bindings annotations.
 */
public class JacksonSdkType<T> extends SdkType<T> {

  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper(new SdkTypeModule());

  private final Class<T> clazz;
  private final Map<String, Variable> variableMap;
  private final Map<String, AnnotatedMember> membersMap;
  private final Map<String, SdkLiteralType<?>> typesMap;

  private JacksonSdkType(
      Class<T> clazz,
      Map<String, Variable> variableMap,
      Map<String, AnnotatedMember> membersMap,
      Map<String, SdkLiteralType<?>> typesMap) {
    this.clazz = requireNonNull(clazz);
    this.variableMap = Map.copyOf(requireNonNull(variableMap));
    this.membersMap = Map.copyOf(requireNonNull(membersMap));
    this.typesMap = Map.copyOf(requireNonNull(typesMap));
  }

  /**
   * Returns a {@link org.flyte.flytekit.SdkType} for {@code clazz}.
   *
   * @param clazz the java type for this {@link org.flyte.flytekit.SdkType}.
   * @return the sdk type
   * @throws IllegalArgumentException when Jackson cannot find a serializer for the supplied type.
   *     For example, it is not an {@link com.google.auto.value.AutoValue} or Jackson cannot
   *     discover properties or constructors.
   */
  public static <T> JacksonSdkType<T> of(Class<T> clazz) {
    try {
      // preemptively check that serializer is known to throw exceptions earlier
      SerializerProvider serializerProvider = OBJECT_MAPPER.getSerializerProviderInstance();

      JsonSerializer<Object> serializer =
          serializerProvider.findTypedValueSerializer(clazz, true, /* property= */ null);

      if (serializerProvider.isUnknownTypeSerializer(serializer)) {
        serializerProvider.reportBadDefinition(
            clazz,
            String.format(
                "No serializer found for class %s and no properties discovered to create BeanSerializer",
                clazz.getName()));
      }

      RootFormatVisitor visitor =
          new RootFormatVisitor(OBJECT_MAPPER.getSerializerProviderInstance());
      serializer.acceptJsonFormatVisitor(
          visitor, OBJECT_MAPPER.getTypeFactory().constructType(clazz));

      return new JacksonSdkType<>(
          clazz, visitor.getVariableMap(), visitor.getMembersMap(), visitor.getTypesMap());
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to find serializer for [%s]", clazz.getName()), e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, Literal> toLiteralMap(T value) {
    try {
      JsonNode tree = OBJECT_MAPPER.valueToTree(value);

      Map<String, LiteralType> literalTypeMap =
          getVariableMap().entrySet().stream()
              .collect(toMap(Map.Entry::getKey, x -> x.getValue().literalType()));

      // The previous trick with JavaType and withValueHandler didn't work because
      // Jackson caches serializers, without considering valueHandler as significant part
      // of the caching key.

      JsonParser tokens = OBJECT_MAPPER.treeAsTokens(tree);
      tokens.nextToken();

      LiteralMapDeserializer deserializer = new LiteralMapDeserializer(literalTypeMap);

      // this is how OBJECT_MAPPER creates deserialization context, otherwise, nested deserializers
      // don't work
      DefaultDeserializationContext ctx =
          ((DefaultDeserializationContext) OBJECT_MAPPER.getDeserializationContext())
              .createInstance(
                  OBJECT_MAPPER.getDeserializationConfig(),
                  tokens,
                  OBJECT_MAPPER.getInjectableValues());

      JacksonLiteralMap jacksonLiteralMap = deserializer.deserialize(tokens, ctx);

      return jacksonLiteralMap.getLiteralMap();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, Variable> getVariableMap() {
    return variableMap;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, SdkLiteralType<?>> toLiteralTypes() {
    return typesMap;
  }

  private Map<String, AnnotatedMember> getMembersMap() {
    return membersMap;
  }

  /** {@inheritDoc} */
  @Override
  public T fromLiteralMap(Map<String, Literal> value) {
    try {
      Map<String, LiteralType> literalTypeMap =
          getVariableMap().entrySet().stream()
              .collect(toMap(Map.Entry::getKey, x -> x.getValue().literalType()));

      JsonNode tree = OBJECT_MAPPER.valueToTree(new JacksonLiteralMap(value, literalTypeMap));

      return OBJECT_MAPPER.treeToValue(tree, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("fromLiteralMap failed for [" + clazz.getName() + "]", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public T promiseFor(String nodeId) {
    try {
      Map<String, SdkBindingData<?>> bindingMap =
          typesMap.entrySet().stream()
              .collect(
                  toMap(
                      Map.Entry::getKey,
                      e -> SdkBindingData.promise(e.getValue(), nodeId, e.getKey())));

      JsonNode tree = OBJECT_MAPPER.valueToTree(new JacksonBindingMap(bindingMap));

      SdkTypeModule sdkTypeModule =
          new SdkTypeModule(new CustomSdkBindingDataDeserializers(bindingMap));
      ObjectMapper mapper = createObjectMapper(sdkTypeModule);
      return mapper.treeToValue(tree, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("promiseFor failed for [" + clazz.getName() + "]", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, SdkBindingData<?>> toSdkBindingMap(T value) {
    return getMembersMap().entrySet().stream()
        .map(
            entry -> {
              String attrName = entry.getKey();
              AnnotatedMember member = entry.getValue();
              return Map.entry(attrName, (SdkBindingData<?>) member.getValue(value));
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
