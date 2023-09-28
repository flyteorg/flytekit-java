/*
 * Copyright 2023 Flyte Authors
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
import static org.flyte.flytekit.jackson.ObjectMapperUtils.createObjectMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.flytekit.SdkLiteralType;

/**
 * Implementation of {@link org.flyte.flytekit.SdkLiteralType} for {@link
 * com.google.auto.value.AutoValue}s and other java types with Jackson bindings annotations.
 */
public class JacksonSdkLiteralType<T> extends SdkLiteralType<T> {

  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper(new SdkLiteralTypeModule());

  private static final LiteralType TYPE = LiteralType.ofSimpleType(SimpleType.STRUCT);

  private final Class<T> clazz;

  private JacksonSdkLiteralType(Class<T> clazz) {
    this.clazz = clazz;
  }

  /**
   * Returns a {@link org.flyte.flytekit.SdkLiteralType} for {@code clazz}.
   *
   * @param clazz the java type for this {@link org.flyte.flytekit.SdkLiteralType}.
   * @return the sdk literal type
   * @throws IllegalArgumentException when Jackson cannot find a serializer for the supplied type.
   *     For example, it is not an {@link com.google.auto.value.AutoValue} or Jackson cannot
   *     discover properties or constructors.
   */
  public static <T> JacksonSdkLiteralType<T> of(Class<T> clazz) {
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
      } else if (!(serializer instanceof BeanSerializer)) {
        throw new IllegalArgumentException(
            String.format(
                "Class [%s] not compatible with JacksonSdkLiteralType. Use SdkLiteralType.of instead",
                clazz.getName()));
      }
      return new JacksonSdkLiteralType<>(clazz);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to find serializer for [%s]", clazz.getName()), e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public LiteralType getLiteralType() {
    return TYPE;
  }

  /** {@inheritDoc} */
  @Override
  public Literal toLiteral(T value) {
    if (value == null) {
      return null;
    }

    var tree = OBJECT_MAPPER.valueToTree(value);

    try {
      return OBJECT_MAPPER.treeToValue(tree, Literal.class);
    } catch (IOException e) {
      throw new UncheckedIOException("toLiteral failed for [" + clazz.getName() + "]: " + value, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public T fromLiteral(Literal literal) {
    if (literal == null) {
      return null;
    }
    requireNonNull(literal.scalar(), "Literal is not a struct: " + literal);
    var struct =
        requireNonNull(
            literal.scalar().generic(), "Scalar literal is not a struct: " + literal.scalar());

    try {
      var tree = OBJECT_MAPPER.valueToTree(struct);

      return OBJECT_MAPPER.treeToValue(tree, clazz);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(
          "fromLiteral failed for [" + clazz.getName() + "]: " + literal, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public BindingData toBindingData(T value) {
    if (value == null) {
      return null;
    }
    var struct = toLiteral(value).scalar().generic();
    return BindingData.ofScalar(Scalar.ofGeneric(struct));
  }
}
