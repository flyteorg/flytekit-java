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

// TODO: this class it is not used. We should remove it or even better use it in place of
//  raw literal types in SdkBinding data
public abstract class SdkLiteralType<T> {
  public abstract LiteralType getLiteralType();

  public abstract Literal toLiteral(T value);

  public abstract T fromLiteral(Literal literal);

  public abstract BindingData toBindingData(T value);

  @Override
  public final int hashCode() {
    return getLiteralType().hashCode();
  }

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
