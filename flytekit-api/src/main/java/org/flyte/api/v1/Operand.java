/*
 * Copyright 2020-2021 Flyte Authors
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

/** Defines an operand to a comparison expression. */
@AutoOneOf(Operand.Kind.class)
public abstract class Operand {
  public enum Kind {
    /** A constant. */
    PRIMITIVE,
    /** One of this node's input variables. */
    VAR
  }

  public abstract Kind kind();

  public abstract Primitive primitive();

  public abstract String var();

  public static Operand ofPrimitive(Primitive primitive) {
    return AutoOneOf_Operand.primitive(primitive);
  }

  public static Operand ofVar(String var) {
    return AutoOneOf_Operand.var(var);
  }
}
