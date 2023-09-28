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

import com.google.auto.value.AutoValue;

/** Defines a conjunction expression of two boolean expressions. */
@AutoValue
public abstract class ConjunctionExpression {
  /**
   * Nested conditions. They can be conjoined using AND / OR. Order of evaluation is not important
   * as the operators are commutative.
   */
  public enum LogicalOperator {
    /** Conjunction. */
    AND,
    /** Disjunction. */
    OR
  }

  public abstract LogicalOperator operator();

  public abstract BooleanExpression leftExpression();

  public abstract BooleanExpression rightExpression();

  public static ConjunctionExpression create(
      LogicalOperator operator,
      BooleanExpression leftExpression,
      BooleanExpression rightExpression) {
    return new AutoValue_ConjunctionExpression(operator, leftExpression, rightExpression);
  }
}
