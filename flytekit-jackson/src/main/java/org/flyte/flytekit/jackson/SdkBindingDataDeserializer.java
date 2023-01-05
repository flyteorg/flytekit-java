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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
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
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.flytekit.SdkBindingData;

class SdkBindingDataDeserializer extends StdDeserializer<SdkBindingData<?>>
    implements ContextualDeserializer {
  private static final long serialVersionUID = 0L;
  private JavaType type;

  public SdkBindingDataDeserializer() {
    super(SdkBindingData.class);
  }

  @Override
  public JsonDeserializer<?> createContextual(
      DeserializationContext deserializationContext, BeanProperty beanProperty)
      throws JsonMappingException {
    this.type = beanProperty.getType().containedType(0);
    return this;
  }

  private SdkBindingData<?> transform(JsonNode tree) {
    switch (Literal.Kind.valueOf(tree.get("literal").asText())) {
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
            // TODO: We need to implement this
            break;
          case BLOB:
            // TODO: We need to implement this
            break;
        }
        break;
      case COLLECTION:
        Iterator<JsonNode> elements = tree.get("value").elements();
        switch (SimpleType.valueOf(tree.get("type").asText())) {
          case STRING:
            return SdkBindingData.ofBindingCollection(
                generateListFromIterator(elements, JsonNode::asText, SdkBindingData::ofString));
            // TODO: Implement the other case
        }

        break;
      case MAP:
        switch (SimpleType.valueOf(tree.get("type").asText())) {
          case STRING:
            return SdkBindingData.ofStringMap(generateMapFromNode(tree, JsonNode::asText));
            // TODO: Implement the other case
        }
        break;
    }

    return null;
  }

  private <T> Map<String, T> generateMapFromNode(
      JsonNode mapNode, Function<JsonNode, T> transformer) {
    JsonNode node = mapNode.get("value");
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(node.fieldNames(), Spliterator.ORDERED), false)
        .map(name -> Map.entry(name, transformer.apply(node.get(name).get("value"))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private <T> List<SdkBindingData<T>> generateListFromIterator(
      Iterator<JsonNode> iterator,
      Function<JsonNode, T> jsonTransformer,
      Function<T, SdkBindingData<T>> bindingDataTransformer) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .map(node -> bindingDataTransformer.apply(jsonTransformer.apply(node.get("value"))))
        .collect(Collectors.toList());
  }

  @Override
  public SdkBindingData<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode tree = jsonParser.readValueAsTree();
    return transform(tree);
  }
}
