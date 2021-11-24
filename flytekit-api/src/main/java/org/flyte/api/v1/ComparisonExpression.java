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
package org.flyte.api.v1;

import com.google.auto.value.AutoValue;

/**
 * Defines a 2-level tree where the root is a comparison operator and Operands are primitives or
 * known variables. Each expression results in a boolean result.
 */
@AutoValue
public abstract class ComparisonExpression {
  /** Binary operator for each expression. */
  public enum Operator {
    /** 'Equal' operator. */
    EQ,
    /** 'Not equal to' operator. */
    NEQ,
    /** 'Greater than' operator. */
    GT,
    /** 'Greater than or equal to' operator. */
    GTE,
    /** 'Lesser than' operator. */
    LT,
    /** 'Lesser than or equal to' operator. */
    LTE
  }

  public abstract Operator operator();

  public abstract Operand leftValue();

  public abstract Operand rightValue();

  public static Builder builder() {
    return new AutoValue_ComparisonExpression.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder operator(Operator operator);

    public abstract Builder leftValue(Operand leftValue);

    public abstract Builder rightValue(Operand rightValue);

    public abstract ComparisonExpression build();
  }
}
