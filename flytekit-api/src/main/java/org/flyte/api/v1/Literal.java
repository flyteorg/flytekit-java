/*
 * Copyright 2020-2023 Flyte Authors
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
import java.util.List;
import java.util.Map;

/**
 * A simple value. This supports any level of nesting (e.g. array of array of array of Blobs) as
 * well as simple primitives.
 */
@AutoOneOf(Literal.Kind.class)
public abstract class Literal {

  public enum Kind {
    /** A simple value. */
    SCALAR,

    /** A collection of literals to allow nesting. */
    COLLECTION,

    /** A map of strings to literals. */
    MAP
  }

  // TODO: add hash from src/main/proto/flyteidl/core/literals.proto

  public abstract Kind kind();

  public abstract Scalar scalar();

  public abstract List<Literal> collection();

  public abstract Map<String, Literal> map();

  public static Literal ofScalar(Scalar scalar) {
    return AutoOneOf_Literal.scalar(scalar);
  }

  public static Literal ofCollection(List<Literal> collection) {
    return AutoOneOf_Literal.collection(collection);
  }

  public static Literal ofMap(Map<String, Literal> map) {
    return AutoOneOf_Literal.map(map);
  }
}
