/*
 * Copyright 2020-2023 Flyte Authors.
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

import com.google.auto.value.AutoOneOf;

/** A simple value. */
@AutoOneOf(Scalar.Kind.class)
public abstract class Scalar {

  public enum Kind {
    PRIMITIVE,
    GENERIC,
    BLOB
  }

  public abstract Kind kind();

  public abstract Primitive primitive();

  public abstract Struct generic();

  public abstract Blob blob();

  // TODO: add the rest of the cases from src/main/proto/flyteidl/core/literals.proto

  public static Scalar ofPrimitive(Primitive primitive) {
    return AutoOneOf_Scalar.primitive(primitive);
  }

  public static Scalar ofGeneric(Struct generic) {
    return AutoOneOf_Scalar.generic(generic);
  }

  public static Scalar ofBlob(Blob blob) {
    return AutoOneOf_Scalar.blob(blob);
  }
}
