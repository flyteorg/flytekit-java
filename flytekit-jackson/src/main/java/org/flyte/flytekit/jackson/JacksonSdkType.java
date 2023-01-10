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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkType;

import static java.util.stream.Collectors.toMap;

public class JacksonSdkType<T> extends SdkType<T> {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .registerModule(new SdkTypeModule())
          .registerModule(new JavaTimeModule())
          .registerModule(new ParameterNamesModule())
          // TODO: Think about this, this is necessary right now because we are adding literal and
          // scalar inside the JSONode in the case of GENERIC
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final Class<T> clazz;
  private final Map<String, Variable> variableMap;

  private JacksonSdkType(Class<T> clazz, Map<String, Variable> variableMap) {
    this.clazz = Objects.requireNonNull(clazz);
    this.variableMap = Objects.requireNonNull(variableMap);
  }

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

      return new JacksonSdkType<>(clazz, visitor.getVariableMap());
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to find serializer for [%s]", clazz.getName()), e);
    }
  }

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
      DefaultDeserializationContext cctx =
          ((DefaultDeserializationContext) OBJECT_MAPPER.getDeserializationContext())
              .createInstance(
                  OBJECT_MAPPER.getDeserializationConfig(),
                  tokens,
                  OBJECT_MAPPER.getInjectableValues());

      JacksonLiteralMap jacksonLiteralMap = deserializer.deserialize(tokens, cctx);

      return jacksonLiteralMap.getLiteralMap();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Map<String, Variable> getVariableMap() {
    return variableMap;
  }

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

  @Override
  public T promiseFor(String nodeId) {
    try {
      Map<String, SdkBindingData<?>> bindingMap =
              getVariableMap().entrySet().stream()
                      .collect(toMap(
                              Map.Entry::getKey,
                              x -> SdkBindingData.ofOutputReference(nodeId, x.getKey(), x.getValue().literalType())));

      JsonNode tree = OBJECT_MAPPER.valueToTree(new JacksonBindingMap(bindingMap));
      ObjectMapper mapper = new ObjectMapper()
              .registerModule(new SdkTypeModule(new CustomSdkBindingDataDeserializers(bindingMap)))
              .registerModule(new JavaTimeModule())
              .registerModule(new ParameterNamesModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      return mapper.treeToValue(tree, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("promiseFor failed for [" + clazz.getName() + "]", e);
    }
  }
}
