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

import com.fasterxml.jackson.core.JacksonException;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.flytekit.SdkBindingData;

class SdkBindingDataDeserializer extends StdDeserializer<SdkBindingData<?>> {
  private static final long serialVersionUID = 0L;

  public SdkBindingDataDeserializer() {
    super(SdkBindingData.class);
  }

  private SdkBindingData<?> transform(JsonNode tree) {
    Literal.Kind literalKind = Literal.Kind.valueOf(tree.get("literal").asText());
    switch (literalKind) {
      case SCALAR:
        switch (Scalar.Kind.valueOf(tree.get("scalar").asText())) {
          case PRIMITIVE:
            switch (Primitive.Kind.valueOf(tree.get("primitive").asText())) {
              case INTEGER_VALUE:
                return SdkBindingData.ofInteger(tree.get("value").longValue());
              case BOOLEAN_VALUE:
                return SdkBindingData.ofBoolean(tree.get("value").booleanValue());
              case STRING_VALUE:
                return SdkBindingData.ofString(tree.get("value").asText());
              case DURATION:
                return SdkBindingData.ofDuration(Duration.parse(tree.get("value").asText()));
              case DATETIME:
                return SdkBindingData.ofDatetime(Instant.parse(tree.get("value").asText()));
              case FLOAT_VALUE:
                return SdkBindingData.ofFloat(tree.get("value").doubleValue());
            }
            break;
          case GENERIC:
          case BLOB:
            // TODO: We need to implement this
            throw new RuntimeException("not supported");
        }
        break;
      case COLLECTION:
        SimpleType collectionSimpleInnerType = SimpleType.valueOf(tree.get("type").asText());
        Iterator<JsonNode> elements = tree.get("value").elements();
        switch (collectionSimpleInnerType) {
          case STRING:
            return generateListFromIterator(elements, JsonNode::asText, SdkBindingData::ofString);
          case DATETIME:
            return generateListFromIterator(
                elements, (node) -> Instant.parse(node.asText()), SdkBindingData::ofDatetime);
          case DURATION:
            return generateListFromIterator(
                elements, (node) -> Duration.parse(node.asText()), SdkBindingData::ofDuration);
          case INTEGER:
            return generateListFromIterator(elements, JsonNode::asLong, SdkBindingData::ofInteger);
          case FLOAT:
            return generateListFromIterator(elements, JsonNode::asDouble, SdkBindingData::ofFloat);
          case BOOLEAN:
            return generateListFromIterator(
                elements, JsonNode::asBoolean, SdkBindingData::ofBoolean);
          case STRUCT:
            // TODO: need to implement this.
            throw new RuntimeException("not supported");
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Not supported simple type %s. Literal: %s",
                    collectionSimpleInnerType, literalKind.name()));
        }

      case MAP:
        SimpleType mapSimpleInnerType = SimpleType.valueOf(tree.get("type").asText());
        switch (mapSimpleInnerType) {
          case STRING:
            return SdkBindingData.ofStringMap(generateMapFromNode(tree, JsonNode::asText));
          case DATETIME:
            return SdkBindingData.ofDatetimeMap(
                generateMapFromNode(tree, (node) -> Instant.parse(node.asText())));
          case DURATION:
            return SdkBindingData.ofDurationMap(
                generateMapFromNode(tree, (node) -> Duration.parse(node.asText())));
          case INTEGER:
            return SdkBindingData.ofIntegerMap(generateMapFromNode(tree, JsonNode::asLong));
          case FLOAT:
            return SdkBindingData.ofFloatMap(generateMapFromNode(tree, JsonNode::asDouble));
          case BOOLEAN:
            return SdkBindingData.ofBooleanMap(generateMapFromNode(tree, JsonNode::asBoolean));
          case STRUCT:
            // TODO: need to implement this.
            throw new UnsupportedOperationException("not supported");
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Not supported simple type %s. Literal: %s",
                    mapSimpleInnerType, literalKind.name()));
        }

      default:
        throw new UnsupportedOperationException(
            String.format("Not supported literal type %s", literalKind.name()));
    }

    // TODO: Think about it
    throw new IllegalStateException("");
  }

  private <T> Map<String, T> generateMapFromNode(
      JsonNode mapNode, Function<JsonNode, T> jsonTransformer) {
    JsonNode node = mapNode.get("value");
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(node.fieldNames(), Spliterator.ORDERED), false)
        .map(name -> Map.entry(name, jsonTransformer.apply(node.get(name).get("value"))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private <T> SdkBindingData<List<T>> generateListFromIterator(
      Iterator<JsonNode> iterator,
      Function<JsonNode, T> jsonTransformer,
      Function<T, SdkBindingData<T>> bindingDataTransformer) {
    return SdkBindingData.ofBindingCollection(
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .map(node -> bindingDataTransformer.apply(jsonTransformer.apply(node.get("value"))))
            .collect(Collectors.toList()));
  }

  @Override
  public SdkBindingData<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode tree = jsonParser.readValueAsTree();
    return transform(tree);
  }
}
