/*
 * Copyright 2023 Flyte Authors.
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
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.flyte.api.v1.Binary;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.BlobMetadata;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.BlobType.BlobDimensionality;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Scalar.Kind;
import org.flyte.api.v1.SimpleType;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkLiteralType;
import org.flyte.flytekit.SdkLiteralTypes;
import org.flyte.flytekit.jackson.JacksonSdkLiteralType;

class SdkBindingDataDeserializer extends StdDeserializer<SdkBindingData<?>>
    implements ContextualDeserializer {
  private static final long serialVersionUID = 0L;

  private final JavaType type;

  public SdkBindingDataDeserializer() {
    this(null);
  }

  private SdkBindingDataDeserializer(JavaType type) {
    super(SdkBindingData.class);

    this.type = type;
  }

  @Override
  public SdkBindingData<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode tree = jsonParser.readValueAsTree();
    return transform(tree, deserializationContext, type);
  }

  private SdkBindingData<?> transform(
      JsonNode tree, DeserializationContext deserializationContext, JavaType type) {
    Literal.Kind literalKind = Literal.Kind.valueOf(tree.get(LITERAL).asText());
    switch (literalKind) {
      case SCALAR:
        return transformScalar(tree, deserializationContext, type);
      case COLLECTION:
        return transformCollection(tree, deserializationContext, type);

      case MAP:
        return transformMap(tree, deserializationContext, type);

      default:
        throw new UnsupportedOperationException(
            String.format("Not supported literal type %s", literalKind.name()));
    }
  }

  private SdkBindingData<?> transformScalar(
      JsonNode tree, DeserializationContext deserializationContext, JavaType type) {
    Scalar.Kind scalarKind = Scalar.Kind.valueOf(tree.get(SCALAR).asText());
    switch (scalarKind) {
      case PRIMITIVE:
        Primitive.Kind primitiveKind = Primitive.Kind.valueOf(tree.get("primitive").asText());
        switch (primitiveKind) {
          case INTEGER_VALUE:
            return SdkBindingDataFactory.of(tree.get(VALUE).longValue());
          case BOOLEAN_VALUE:
            return SdkBindingDataFactory.of(tree.get(VALUE).booleanValue());
          case STRING_VALUE:
            return SdkBindingDataFactory.of(tree.get(VALUE).asText());
          case DURATION:
            return SdkBindingDataFactory.of(Duration.parse(tree.get(VALUE).asText()));
          case DATETIME:
            return SdkBindingDataFactory.of(Instant.parse(tree.get(VALUE).asText()));
          case FLOAT_VALUE:
            return SdkBindingDataFactory.of(tree.get(VALUE).doubleValue());
        }
        throw new UnsupportedOperationException(
            "Type contains an unsupported primitive: " + primitiveKind);

      case BLOB:
        return transformBlob(tree);

      case GENERIC:
        return transformGeneric(tree, deserializationContext, scalarKind, type);

      case BINARY:
        return transformBinary(tree);

      default:
        throw new UnsupportedOperationException(
            "Type contains an unsupported scalar: " + scalarKind);
    }
  }

  private static SdkBindingData<Binary> transformBinary(JsonNode tree) {
    JsonNode value = tree.get(VALUE);
    String tag = value.get(Binary.TAG_FIELD).asText();
    String base64Value = value.get(Binary.VALUE_FIELD).asText();

    return SdkBindingDataFactory.of(
        Binary.builder().tag(tag).value(Base64.getDecoder().decode(base64Value)).build());
  }

  private static SdkBindingData<Blob> transformBlob(JsonNode tree) {
    JsonNode value = tree.get(VALUE);
    String uri = value.get("uri").asText();
    JsonNode type = value.get("metadata").get("type");
    String format = type.get("format").asText();
    BlobDimensionality dimensionality =
        BlobDimensionality.valueOf(type.get("dimensionality").asText());
    return SdkBindingDataFactory.of(
        Blob.builder()
            .uri(uri)
            .metadata(
                BlobMetadata.builder()
                    .type(BlobType.builder().format(format).dimensionality(dimensionality).build())
                    .build())
            .build());
  }

  private SdkBindingData<Object> transformGeneric(
      JsonNode tree,
      DeserializationContext deserializationContext,
      Kind scalarKind,
      JavaType type) {
    JsonParser jsonParser = tree.get(VALUE).traverse();
    try {
      jsonParser.nextToken();
      Object object =
          deserializationContext
              .findNonContextualValueDeserializer(type)
              .deserialize(jsonParser, deserializationContext);
      @SuppressWarnings("unchecked")
      SdkLiteralType<Object> jacksonSdkLiteralType =
          (SdkLiteralType<Object>) JacksonSdkLiteralType.of(type.getRawClass());
      return SdkBindingData.literal(jacksonSdkLiteralType, object);
    } catch (IOException e) {
      throw new UnsupportedOperationException(
          "Type contains an unsupported generic: " + scalarKind, e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> SdkBindingData<List<T>> transformCollection(
      JsonNode tree, DeserializationContext deserializationContext, JavaType type) {
    SdkLiteralType<T> literalType = (SdkLiteralType<T>) readLiteralType(tree.get(TYPE));
    Iterator<JsonNode> elements = tree.get(VALUE).elements();

    switch (literalType.getLiteralType().getKind()) {
      case SIMPLE_TYPE:
      case MAP_VALUE_TYPE:
      case COLLECTION_TYPE:
      case BLOB_TYPE:
        JavaType realJavaType =
            literalType instanceof JacksonSdkLiteralType ? type.getContentType() : type;
        List<T> collection =
            (List<T>)
                streamOf(elements)
                    .map((JsonNode tree1) -> transform(tree1, deserializationContext, realJavaType))
                    .map(SdkBindingData::get)
                    .collect(toList());
        return SdkBindingDataFactory.of(literalType, collection);

      case SCHEMA_TYPE:
      default:
        throw new UnsupportedOperationException(
            "Type contains a collection of an supported literal type: " + literalType);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> SdkBindingData<Map<String, T>> transformMap(
      JsonNode tree, DeserializationContext deserializationContext, JavaType type) {
    SdkLiteralType<T> literalType = (SdkLiteralType<T>) readLiteralType(tree.get(TYPE));
    JsonNode valueNode = tree.get(VALUE);
    List<Map.Entry<String, JsonNode>> entries =
        streamOf(valueNode.fieldNames())
            .map(name -> Map.entry(name, valueNode.get(name)))
            .collect(toList());
    switch (literalType.getLiteralType().getKind()) {
      case SIMPLE_TYPE:
      case MAP_VALUE_TYPE:
      case COLLECTION_TYPE:
      case BLOB_TYPE:
        JavaType realJavaType =
            literalType instanceof JacksonSdkLiteralType ? type.getContentType() : type;
        Map<String, T> bindingDataMap =
            entries.stream()
                .map(
                    entry ->
                        Map.entry(
                            entry.getKey(),
                            (T)
                                transform(entry.getValue(), deserializationContext, realJavaType)
                                    .get()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return SdkBindingDataFactory.of(literalType, bindingDataMap);

      case SCHEMA_TYPE:
      default:
        throw new UnsupportedOperationException(
            "Type contains a map of an supported literal type: " + literalType);
    }
  }

  private SdkLiteralType<?> readLiteralType(JsonNode typeNode) {
    LiteralType.Kind kind = LiteralType.Kind.valueOf(typeNode.get(KIND).asText());
    switch (kind) {
      case SIMPLE_TYPE:
        SimpleType simpleType = SimpleType.valueOf(typeNode.get(VALUE).asText());
        switch (simpleType) {
          case INTEGER:
            return SdkLiteralTypes.integers();
          case FLOAT:
            return SdkLiteralTypes.floats();
          case STRING:
            return SdkLiteralTypes.strings();
          case BOOLEAN:
            return SdkLiteralTypes.booleans();
          case DATETIME:
            return SdkLiteralTypes.datetimes();
          case DURATION:
            return SdkLiteralTypes.durations();
          case STRUCT:
            return JacksonSdkLiteralType.of(type.getContentType().getRawClass());
          case BINARY:
            return SdkLiteralTypes.binary();
        }
        throw new UnsupportedOperationException(
            "Type contains a collection/map of an supported literal type: " + kind);
      case MAP_VALUE_TYPE:
        return SdkLiteralTypes.maps(readLiteralType(typeNode.get(VALUE).get(TYPE)));
      case COLLECTION_TYPE:
        return SdkLiteralTypes.collections(readLiteralType(typeNode.get(VALUE).get(TYPE)));
      case BLOB_TYPE:
        return SdkLiteralTypes.blobs(
            BlobType.builder()
                .format(typeNode.get(VALUE).get("format").asText())
                .dimensionality(
                    BlobDimensionality.valueOf(typeNode.get(VALUE).get("dimensionality").asText()))
                .build());
      case SCHEMA_TYPE:
      default:
        throw new UnsupportedOperationException(
            "Type contains a collection/map of an supported literal type: " + kind);
    }
  }

  private <T> Stream<T> streamOf(Iterator<T> nodes) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(nodes, Spliterator.ORDERED), false);
  }

  @Override
  public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
    return new SdkBindingDataDeserializer(property.getType().containedType(0));
  }
}
