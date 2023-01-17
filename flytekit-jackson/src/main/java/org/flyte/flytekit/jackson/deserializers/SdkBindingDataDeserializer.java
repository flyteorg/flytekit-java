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
package org.flyte.flytekit.jackson.deserializers;

import static java.util.stream.Collectors.toList;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.KIND;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.LITERAL;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.SCALAR;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.TYPE;
import static org.flyte.flytekit.jackson.serializers.SdkBindingDataSerializationProtocol.VALUE;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.flytekit.SdkBindingData;

class SdkBindingDataDeserializer extends StdDeserializer<SdkBindingData<?>> {
  private static final long serialVersionUID = 0L;

  public SdkBindingDataDeserializer() {
    super(SdkBindingData.class);
  }

  private SdkBindingData<?> transform(JsonNode tree) {
    Literal.Kind literalKind = Literal.Kind.valueOf(tree.get(LITERAL).asText());
    switch (literalKind) {
      case SCALAR:
        switch (Scalar.Kind.valueOf(tree.get(SCALAR).asText())) {
          case PRIMITIVE:
            switch (Primitive.Kind.valueOf(tree.get("primitive").asText())) {
              case INTEGER_VALUE:
                return SdkBindingData.ofInteger(tree.get(VALUE).longValue());
              case BOOLEAN_VALUE:
                return SdkBindingData.ofBoolean(tree.get(VALUE).booleanValue());
              case STRING_VALUE:
                return SdkBindingData.ofString(tree.get(VALUE).asText());
              case DURATION:
                return SdkBindingData.ofDuration(Duration.parse(tree.get(VALUE).asText()));
              case DATETIME:
                return SdkBindingData.ofDatetime(Instant.parse(tree.get(VALUE).asText()));
              case FLOAT_VALUE:
                return SdkBindingData.ofFloat(tree.get(VALUE).doubleValue());
            }
            break;
          case GENERIC:
          case BLOB:
            // TODO: We need to implement this
            throw new RuntimeException("not supported");
        }
        break;
      case COLLECTION:
        return transformCollection(tree);

      case MAP:
        return transformMap(tree);

      default:
        throw new UnsupportedOperationException(
            String.format("Not supported literal type %s", literalKind.name()));
    }

    // TODO: Think about it
    throw new IllegalStateException("");
  }

  @SuppressWarnings("unchecked")
  private <T> SdkBindingData<List<T>> transformCollection(JsonNode tree) {
    LiteralType.Kind kind = LiteralType.Kind.valueOf(tree.get(TYPE).get(KIND).asText());
    Iterator<JsonNode> elements = tree.get(VALUE).elements();
    switch (kind) {
      case SCHEMA_TYPE:
      case BLOB_TYPE:
        // TODO: We need to implement this
        throw new RuntimeException("not supported");
      case SIMPLE_TYPE:
      case MAP_VALUE_TYPE:
      case COLLECTION_TYPE:
        List<? extends SdkBindingData<?>> x =
            streamOf(elements).map(this::transform).collect(toList());

        return SdkBindingData.ofBindingCollection((List<SdkBindingData<T>>) x);
    }
    return null; // TODO throw exception
  }

  @SuppressWarnings("unchecked")
  private <T> SdkBindingData<Map<String, T>> transformMap(JsonNode tree) {
    LiteralType.Kind kind = LiteralType.Kind.valueOf(tree.get(TYPE).get(KIND).asText());
    JsonNode valueNode = tree.get(VALUE);
    List<Map.Entry<String, JsonNode>> entries =
        streamOf(valueNode.fieldNames())
            .map(name -> Map.entry(name, valueNode.get(name)))
            .collect(toList());
    switch (kind) {
      case SCHEMA_TYPE:
      case BLOB_TYPE:
        // TODO: We need to implement this
        throw new RuntimeException("not supported");
      case SIMPLE_TYPE:
      case MAP_VALUE_TYPE:
      case COLLECTION_TYPE:
        // TODO code similar to generateMapFromNode
        Map<String, SdkBindingData<T>> bindingDataMap =
            entries.stream()
                .map(
                    entry ->
                        Map.entry(entry.getKey(), (SdkBindingData<T>) transform(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return SdkBindingData.ofBindingMap(bindingDataMap);
    }
    return null; // TODO throw exception
  }

  private <T> Stream<T> streamOf(Iterator<T> nodes) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(nodes, Spliterator.ORDERED), false);
  }

  @Override
  public SdkBindingData<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode tree = jsonParser.readValueAsTree();
    return transform(tree);
  }
}
