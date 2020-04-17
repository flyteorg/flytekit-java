/*
 * Copyright 2020 Spotify AB.
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

import java.lang.reflect.Type;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.TypeToken;

/**
 * A {@link Type} with generics.
 *
 * <p>There are two ways to get a {@code TypeDescriptor} instance:
 *
 * <ul>
 *   <li>Capture a generic type with a subclass. For example:
 *       <pre>{@code new TypeToken<List<String>>() {}}</pre>
 *   <li>Wrap a {@code Type} obtained via reflection. For example: {@code TypeDescriptor.of(cls)}
 * </ul>
 *
 * @param <T> type
 */
public abstract class TypeDescriptor<T> {

  // TypeDescriptor is just a wrapper for TypeToken not to have guava in API surface
  private final TypeToken<T> token;

  protected TypeDescriptor(TypeToken<T> token) {
    this.token = token;
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter {@code T}, which should
   * resolve to a concrete type in the context of the class {@code clazz}.
   *
   * @param clazz underling type
   */
  @SuppressWarnings("unchecked")
  protected TypeDescriptor(Class<?> clazz) {
    TypeToken<T> unresolvedToken = new TypeToken<T>(getClass()) {};
    token = (TypeToken<T>) TypeToken.of(clazz).resolveType(unresolvedToken.getType());
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter {@code T}. To use this
   * Constructs a new type token of {@code T}.
   *
   * <p>Clients create an empty anonymous subclass. Doing so embeds the type parameter in the
   * anonymous class's type hierarchy so we can reconstitute it at runtime despite erasure.
   *
   * <p>For example:
   *
   * <pre>{@code
   * TypeDescriptor<List<String>> t = new TypeDescriptor<List<String>>() {};
   * }</pre>
   */
  protected TypeDescriptor() {
    token = new TypeToken<T>(getClass()) {};
  }

  /**
   * Returns an instance of type descriptor token that wraps {@code type}.
   *
   * @param <T> type
   * @param type type
   * @return type descriptor
   */
  public static <T> TypeDescriptor<T> of(Class<T> type) {
    return new SimpleTypeDescriptor<>(TypeToken.of(type));
  }

  /**
   * Returns an instance of type descriptor token that wraps {@code type}.
   *
   * @param type type
   * @return type descriptor
   */
  public static TypeDescriptor<?> of(Type type) {
    return new SimpleTypeDescriptor<>(TypeToken.of(type));
  }

  /**
   * Returns the raw type of {@code T}.
   *
   * @return raw type
   */
  public Class<? super T> getRawType() {
    return token.getRawType();
  }

  /**
   * A non-abstract {@link TypeDescriptor} for construction directly from an existing {@link
   * TypeToken}.
   */
  private static final class SimpleTypeDescriptor<T> extends TypeDescriptor<T> {
    SimpleTypeDescriptor(TypeToken<T> typeToken) {
      super(typeToken);
    }
  }
}
