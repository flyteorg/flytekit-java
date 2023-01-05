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
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
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
            //TODO: We need to implement this
            break;
          case BLOB:
            //TODO: We need to implement this
            break;
        }
        break;
      case COLLECTION:
        //TODO: We need to recover the inner element type from the tree.get("type").

        List<SdkBindingData<?>> bindingDataList =
            StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    tree.get("value").elements(), Spliterator.ORDERED),
                false).map(this::transform).collect(Collectors.toList());

        return null;
      case MAP:
        //TODO: We need to implement this
        break;
    }

    return null;
  }

  @Override
  public SdkBindingData<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode tree = jsonParser.readValueAsTree();
    return transform(tree);
  }
}
