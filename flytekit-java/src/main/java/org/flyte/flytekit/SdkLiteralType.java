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
package org.flyte.flytekit;

import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

/**
 * Bridge between the a Java type and a variable in Flyte.
 *
 * @param <T> the Java native type to bridge.
 */
public abstract class SdkLiteralType<T> {
  /**
   * Returns the {@link LiteralType} corresponding to this type.
   *
   * @return the literal type.
   */
  public abstract LiteralType getLiteralType();

  /**
   * Coverts the value into a {@link Literal}.
   *
   * @param value value to convert.
   * @return the literal.
   */
  public abstract Literal toLiteral(T value);

  /**
   * Coverts a {@link Literal} into a value.
   *
   * @param literal literal to convert.
   * @return the value.
   */
  public abstract T fromLiteral(Literal literal);

  /**
   * Coverts the value into a {@link BindingData}.
   *
   * @param value value to convert.
   * @return the binding data.
   */
  public abstract BindingData toBindingData(T value);

  /**
   * {@inheritDoc}
   *
   * <p>Hashcode is computed based on {@link #getLiteralType()}
   */
  @Override
  public final int hashCode() {
    return getLiteralType().hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Equals comparing only {@link #getLiteralType()}. Simplifies equality among the several
   * implementation of this class.
   */
  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof SdkLiteralType) {
      return this.getLiteralType().equals(((SdkLiteralType<?>) obj).getLiteralType());
    }
    return false;
  }
}
