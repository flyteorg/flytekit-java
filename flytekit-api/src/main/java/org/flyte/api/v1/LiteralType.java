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
package org.flyte.api.v1;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Defines a strong type to allow type checking between interfaces. */
@AutoValue
public abstract class LiteralType {

  @Nullable
  public abstract SimpleType simpleType();

  public static LiteralType create(SimpleType simpleType) {
    return new AutoValue_LiteralType(simpleType);
  }
}
