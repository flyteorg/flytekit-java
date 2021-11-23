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
package org.flyte.utils;

import flyteidl.core.Literals;
import java.util.Map;

public class Literal {

  public static Literals.LiteralMap ofIntegerMap(Map<String, Long> map) {
    Literals.LiteralMap.Builder builder = Literals.LiteralMap.newBuilder();

    map.forEach((key, value) -> builder.putLiterals(key, ofInteger(value)));

    return builder.build();
  }

  public static Literals.LiteralMap ofStringMap(Map<String, String> map) {
    Literals.LiteralMap.Builder builder = Literals.LiteralMap.newBuilder();

    map.forEach((key, value) -> builder.putLiterals(key, ofString(value)));

    return builder.build();
  }

  public static Literals.Literal ofInteger(long value) {
    Literals.Primitive primitive = Literals.Primitive.newBuilder().setInteger(value).build();
    Literals.Scalar scalar = Literals.Scalar.newBuilder().setPrimitive(primitive).build();

    return Literals.Literal.newBuilder().setScalar(scalar).build();
  }

  public static Literals.Literal ofString(String value) {
    Literals.Primitive primitive = Literals.Primitive.newBuilder().setStringValue(value).build();
    Literals.Scalar scalar = Literals.Scalar.newBuilder().setPrimitive(primitive).build();

    return Literals.Literal.newBuilder().setScalar(scalar).build();
  }
}
