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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
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
        SdkBindingData<?> tree1 = transformCollection(tree);
        if (tree1 != null) return tree1;


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
                String.format("Not supported simple type %s.", mapSimpleInnerType));
        }

      default:
        throw new UnsupportedOperationException(
            String.format("Not supported literal type %s", literalKind.name()));
    }

    // TODO: Think about it
    throw new IllegalStateException("");
  }

  private <T> SdkBindingData<List<T>> transformCollection(JsonNode tree) {
    /*

    */
    LiteralType.Kind kind = LiteralType.Kind.valueOf(tree.get("type").get("kind").asText());
    Iterator<JsonNode> elements = tree.get("value").elements();
    switch (kind) {
      case SIMPLE_TYPE:
        return (SdkBindingData<List<T>>) transformSimpleType(tree, elements);
      case SCHEMA_TYPE:
      case BLOB_TYPE:
        // TODO: We need to implement this
        throw new RuntimeException("not supported");
      case COLLECTION_TYPE:
        List<? extends SdkBindingData<?>> x = streamOf(elements)
                .map(this::transform)
                .collect(Collectors.toList());

        return SdkBindingData.ofBindingCollection((List<SdkBindingData<T>>)x);
      case MAP_VALUE_TYPE:
        break;
    }
    return null;
  }

  private SdkBindingData<?> transformSimpleType(JsonNode tree, Iterator<JsonNode> elements) {
    SimpleType collectionSimpleInnerType = SimpleType.valueOf(tree.get("type").get("value").asText());
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
                String.format("Not supported simple type %s.", collectionSimpleInnerType));
    }
  }

    private <T> Map<String, T> generateMapFromNode(
      JsonNode mapNode, Function<JsonNode, T> jsonTransformer) {
    JsonNode node = mapNode.get("value");
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(node.fieldNames(), Spliterator.ORDERED), false)
        .map(name -> Map.entry(name, jsonTransformer.apply(node.get(name).get("value"))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private <T> Stream<T> streamOf(Iterator<T> nodes) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(nodes, Spliterator.ORDERED), false);
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
